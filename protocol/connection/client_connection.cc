/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include "config.h"

#include "client_connection.h"
#include "cJSON_utils.h"
#include "client_mcbp_commands.h"

#include <cbsasl/client.h>
#include <mcbp/mcbp.h>
#include <memcached/protocol_binary.h>
#include <nlohmann/json.hpp>
#include <platform/compress.h>
#include <platform/dirutils.h>
#include <platform/socket.h>
#include <platform/strerror.h>

#include <cerrno>
#include <gsl/gsl>
#include <iostream>
#include <limits>
#include <memory>
#ifndef WIN32
#include <netdb.h>
#include <netinet/tcp.h> // For TCP_NODELAY etc
#endif
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>

static const bool packet_dump = getenv("COUCHBASE_PACKET_DUMP") != nullptr;

::std::ostream& operator<<(::std::ostream& os, const DocumentInfo& info) {
    return os << "id:" << info.id << " flags:" << info.flags
              << " exp:" << info.expiration
              << " datatype:" << int(info.datatype) << " cas:" << info.cas;
}

::std::ostream& operator<<(::std::ostream& os, const Document& doc) {
    os << "info:" << doc.info << " value: [" << std::hex;
    for (auto& v : doc.value) {
        os << int(v) << " ";
    }
    return os << std::dec << "]";
}

void Document::compress() {
    if (mcbp::datatype::is_snappy(protocol_binary_datatype_t(info.datatype))) {
        throw std::invalid_argument(
                "Document::compress: Cannot compress already compressed "
                "document.");
    }

    cb::compression::Buffer buf;
    cb::compression::deflate(cb::compression::Algorithm::Snappy, value, buf);
    value = {buf.data(), buf.size()};
    info.datatype = cb::mcbp::Datatype(int(info.datatype) |
                                       int(cb::mcbp::Datatype::Snappy));
}

/////////////////////////////////////////////////////////////////////////
// Implementation of the MemcachedConnection class
/////////////////////////////////////////////////////////////////////////
MemcachedConnection::MemcachedConnection(const std::string& host,
                                         in_port_t port,
                                         sa_family_t family,
                                         bool ssl)
    : host(host),
      port(port),
      family(family),
      ssl(ssl),
      context(nullptr),
      bio(nullptr),
      sock(INVALID_SOCKET),
      synchronous(false) {
    if (ssl) {
        char* env = getenv("COUCHBASE_SSL_CLIENT_CERT_PATH");
        if (env != nullptr) {
            setSslCertFile(std::string{env} + "/client.pem");
            setSslKeyFile(std::string{env} + "/client.key");
        }
    }
}

MemcachedConnection::~MemcachedConnection() {
    close();
}

void MemcachedConnection::close() {
    effective_features.clear();
    if (ssl) {
        if (bio != nullptr) {
            BIO_free_all(bio);
            bio = nullptr;
        }
        if (context != nullptr) {
            SSL_CTX_free(context);
            context = nullptr;
        }
    }

    if (sock != INVALID_SOCKET) {
        cb::net::closesocket(sock);
        sock = INVALID_SOCKET;
    }
}

SOCKET try_connect_socket(struct addrinfo* next,
                          const std::string& hostname,
                          in_port_t port) {
    SOCKET sfd = cb::net::socket(
            next->ai_family, next->ai_socktype, next->ai_protocol);
    if (sfd == INVALID_SOCKET) {
        throw std::system_error(cb::net::get_socket_error(),
                                std::system_category(),
                                "socket() failed (" + hostname + " " +
                                        std::to_string(port) + ")");
    }

#ifdef WIN32
    // BIO_new_socket pass the socket as an int, but it is a SOCKET on
    // Windows.. On windows a socket is an unsigned value, and may
    // get an overflow inside openssl (I don't know the exact width of
    // the SOCKET, and how openssl use the value internally). This
    // class is mostly used from the test framework so let's throw
    // an exception instead and treat it like a test failure (to be
    // on the safe side). We'll be refactoring to SCHANNEL in the
    // future anyway.
    if (sfd > std::numeric_limits<int>::max()) {
        cb::net::closesocket(sfd);
        throw std::runtime_error(
                "Socket value too big "
                "(may trigger behavior openssl)");
    }
#endif

    // When running unit tests on our Windows CV system we somtimes
    // see connect fail with WSAEADDRINUSE. For a client socket
    // we don't bind the socket as that's implicit from calling
    // connect. Mark the socket reusable so that the kernel may
    // reuse the socket earlier
    const int flag = 1;
    cb::net::setsockopt(sfd,
                        SOL_SOCKET,
                        SO_REUSEADDR,
                        reinterpret_cast<const void*>(&flag),
                        sizeof(flag));

    // Try to set the nodelay mode on the socket (but ignore
    // if we fail to do so..
    cb::net::setsockopt(sfd,
                        IPPROTO_TCP,
                        TCP_NODELAY,
                        reinterpret_cast<const void*>(&flag),
                        sizeof(flag));

    if (cb::net::connect(sfd, next->ai_addr, next->ai_addrlen) == SOCKET_ERROR) {
        auto error = cb::net::get_socket_error();
        cb::net::closesocket(sfd);
#ifdef WIN32
        WSASetLastError(error);
#endif
        throw std::system_error(error,
                                std::system_category(),
                                "connect() failed (" + hostname + " " +
                                        std::to_string(port) + ")");
    }

    // Socket is connected and ready to use
    return sfd;
}

SOCKET cb::net::new_socket(const std::string& host,
                           in_port_t port,
                           sa_family_t family) {
    struct addrinfo hints = {};
    hints.ai_flags = AI_PASSIVE;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = family;

    int error;
    struct addrinfo* ai;
    std::string hostname{host};

    if (hostname.empty() || hostname == "localhost") {
        if (family == AF_INET) {
            hostname.assign("127.0.0.1");
        } else if (family == AF_INET6){
            hostname.assign("::1");
        } else if (family == AF_UNSPEC) {
            hostname.assign("localhost");
        }
    }

    error = getaddrinfo(
            hostname.c_str(), std::to_string(port).c_str(), &hints, &ai);

    if (error != 0) {
        throw std::system_error(error,
                                std::system_category(),
                                "Failed to resolve address host: \"" +
                                        hostname +
                                        "\" Port: " + std::to_string(port));
    }

    bool unit_tests = getenv("MEMCACHED_UNIT_TESTS") != nullptr;

    // Iterate over all of the entries returned by getaddrinfo
    // and try to connect to them. Depending on the input data we
    // might get multiple returns (ex: localhost with AF_UNSPEC returns
    // both IPv4 and IPv6 address, and IPv4 could fail while IPv6
    // might succeed.
    for (auto* next = ai; next; next = next->ai_next) {
        int retry = unit_tests ? 200 : 0;
        do {
            try {
                auto sfd = try_connect_socket(next, hostname, port);
                freeaddrinfo(ai);
                return sfd;
            } catch (const std::system_error& error) {
                if (unit_tests) {
                    std::cerr << "Failed building socket: " << error.what()
                              << std::endl;
#ifndef WIN32
                    const int WSAEADDRINUSE = EADDRINUSE;
#endif
                    if (error.code().value() == WSAEADDRINUSE) {
                        std::cerr << "EADDRINUSE.. backing off" << std::endl;
                        std::this_thread::sleep_for(
                                std::chrono::milliseconds(10));
                    } else {
                        // Not subject for backoff and retry
                        retry = 0;
                    }
                }
            }
        } while (retry-- > 0);
        // Try next entry returned from getaddinfo
    }

    freeaddrinfo(ai);
    return INVALID_SOCKET;
}

std::tuple<SOCKET, SSL_CTX*, BIO*> cb::net::new_ssl_socket(
        const std::string& host,
        in_port_t port,
        sa_family_t family,
        const std::string& ssl_cert_file,
        const std::string& ssl_key_file) {
    auto sock = cb::net::new_socket(host, port, family);
    if (sock == INVALID_SOCKET) {
        return std::tuple<SOCKET, SSL_CTX*, BIO*>{
                INVALID_SOCKET, nullptr, nullptr};
    }

    /* we're connected */
    auto* context = SSL_CTX_new(SSLv23_client_method());
    if (context == nullptr) {
        throw std::runtime_error("Failed to create openssl client context");
    }

    if (!ssl_cert_file.empty() && !ssl_key_file.empty()) {
        if (!SSL_CTX_use_certificate_file(
                    context, ssl_cert_file.c_str(), SSL_FILETYPE_PEM) ||
            !SSL_CTX_use_PrivateKey_file(
                    context, ssl_key_file.c_str(), SSL_FILETYPE_PEM) ||
            !SSL_CTX_check_private_key(context)) {
            std::vector<char> ssl_err(1024);
            ERR_error_string_n(ERR_get_error(), ssl_err.data(), ssl_err.size());
            SSL_CTX_free(context);
            throw std::runtime_error(
                    std::string("Failed to use SSL cert and key: ") +
                    ssl_err.data());
        }
    }

    // Ensure read/write operations only return after the
    // handshake and successful completion.
    SSL_CTX_set_mode(context, SSL_MODE_AUTO_RETRY);

    BIO* bio = BIO_new_ssl(context, 1);
    BIO_push(bio, BIO_new_socket(gsl::narrow<int>(sock), 0));

    if (BIO_do_handshake(bio) <= 0) {
        BIO_free_all(bio);
        SSL_CTX_free(context);
        throw std::runtime_error("Failed to do SSL handshake!");
    }

    return std::tuple<SOCKET, SSL_CTX*, BIO*>{sock, context, bio};
}

void MemcachedConnection::connect() {
    if (bio != nullptr) {
        BIO_free_all(bio);
        bio = nullptr;
    }

    if (context != nullptr) {
        SSL_CTX_free(context);
    }

    if (ssl) {
        std::tie(sock, context, bio) = cb::net::new_ssl_socket(
                host, port, family, ssl_cert_file, ssl_key_file);
    } else {
        sock = cb::net::new_socket(host, port, family);
    }

    if (sock == INVALID_SOCKET) {
        auto error = cb::net::get_socket_error();
        std::string msg("Failed to connect to: ");
        if (family == AF_INET || family == AF_UNSPEC) {
            if (host.empty()) {
                msg += "localhost:";
            } else {
                msg += host + ":";
            }
        } else {
            if (host.empty()) {
                msg += "[::1]:";
            } else {
                msg += "[" + host + "]:";
            }
        }
        msg.append(std::to_string(port));
        throw std::system_error(error, std::system_category(), msg);
    }

    bool unitTests = getenv("MEMCACHED_UNIT_TESTS") != nullptr;
    if (!ssl && unitTests) {
        // Enable LINGER with zero timeout. This changes the
        // behaviour of close() - any unsent data will be
        // discarded, and the connection will be immediately
        // closed with a RST, and is immediately destroyed.  This
        // has the advantage that the socket doesn't enter
        // TIME_WAIT; and hence doesn't consume an emphemeral port
        // until it times out (default 60s).
        //
        // By using LINGER we (hopefully!) avoid issues in CV jobs
        // where ephemeral ports are exhausted and hence tests
        // intermittently fail. One minor downside the RST
        // triggers a warning in the server side logs: 'read
        // error: Connection reset by peer'.
        //
        // Note that this isn't enabled for SSL sockets, which don't
        // appear to be happy with having the underlying socket closed
        // immediately; I suspect due to the additional out-of-band
        // messages SSL may send/recv in addition to normal traffic.
        struct linger sl;
        sl.l_onoff = 1;
        sl.l_linger = 0;
        cb::net::setsockopt(sock,
                            SOL_SOCKET,
                            SO_LINGER,
                            reinterpret_cast<const void*>(&sl),
                            sizeof(sl));
    }
}

void MemcachedConnection::sendBufferSsl(cb::const_byte_buffer buf) {
    const char* data = reinterpret_cast<const char*>(buf.data());
    cb::const_byte_buffer::size_type nbytes = buf.size();
    cb::const_byte_buffer::size_type offset = 0;

    while (offset < nbytes) {
        int nw = BIO_write(
                bio, data + offset, gsl::narrow<int>(nbytes - offset));
        if (nw <= 0) {
            if (BIO_should_retry(bio) == 0) {
                throw std::runtime_error(
                        "Failed to write data, BIO_write returned " +
                        std::to_string(nw));
            }
        } else {
            offset += nw;
        }
    }
}

void MemcachedConnection::sendBufferSsl(const std::vector<iovec>& list) {
    for (auto buf : list) {
        sendBufferSsl({reinterpret_cast<uint8_t*>(buf.iov_base), buf.iov_len});
    }
}

void MemcachedConnection::sendBufferPlain(cb::const_byte_buffer buf) {
    const char* data = reinterpret_cast<const char*>(buf.data());
    cb::const_byte_buffer::size_type nbytes = buf.size();
    cb::const_byte_buffer::size_type offset = 0;

    while (offset < nbytes) {
        auto nw = cb::net::send(sock, data + offset, nbytes - offset, 0);
        if (nw <= 0) {
            throw std::system_error(
                    cb::net::get_socket_error(),
                    std::system_category(),
                    "MemcachedConnection::sendFramePlain: failed to send data");
        } else {
            offset += nw;
        }
    }
}

void MemcachedConnection::sendBufferPlain(const std::vector<iovec>& iov) {
    // Calculate total size.
    int bytes_remaining = 0;
    for (const auto& io : iov) {
        bytes_remaining += int(io.iov_len);
    }

    // Encode sendmsg() message header.
    msghdr msg;
    std::memset(&msg, 0, sizeof(msg));
    // sendmsg() doesn't actually change the value of msg_iov; but as
    // it's a C API it doesn't have a const modifier. Therefore need
    // to cast away const.
    msg.msg_iov = const_cast<iovec*>(iov.data());
    msg.msg_iovlen = int(iov.size());

    // repeatedly call sendmsg() until the complete payload has been
    // transmitted.
    for (;;) {
        auto bytes_sent = cb::net::sendmsg(sock, &msg, 0);
        if (bytes_sent < 0) {
            throw std::system_error(cb::net::get_socket_error(),
                                    std::system_category(),
                                    "MemcachedConnection::sendBufferPlain: "
                                    "sendmsg() failed to send data");
        }

        bytes_remaining -= bytes_sent;
        if (bytes_remaining == 0) {
            // All data sent.
            return;
        }

        // Partial send. Remove the completed iovec entries from the
        // list of pending writes.
        while ((msg.msg_iovlen > 0) &&
               (bytes_sent >= ssize_t(msg.msg_iov->iov_len))) {
            // Complete element consumed; update msg_iov / iovlen to next
            // element.
            bytes_sent -= (ssize_t)msg.msg_iov->iov_len;
            msg.msg_iovlen--;
            msg.msg_iov++;
        }

        // Might have written just part of the last iovec entry;
        // adjust it so the next write will do the rest.
        if (bytes_sent > 0) {
            msg.msg_iov->iov_base =
                    (void*)((unsigned char*)msg.msg_iov->iov_base + bytes_sent);
            msg.msg_iov->iov_len -= bytes_sent;
        }
    }
}

void MemcachedConnection::readSsl(Frame& frame, size_t bytes) {
    Frame::size_type offset = frame.payload.size();
    frame.payload.resize(bytes + offset);
    char* data = reinterpret_cast<char*>(frame.payload.data()) + offset;

    size_t total = 0;

    while (total < bytes) {
        int nr = BIO_read(bio, data + total, gsl::narrow<int>(bytes - total));
        if (nr <= 0) {
            if (BIO_should_retry(bio) == 0) {
                throw std::runtime_error(
                        "Failed to read data, BIO_read returned " +
                        std::to_string(nr));
            }
        } else {
            total += nr;
        }
    }
}

void MemcachedConnection::readPlain(Frame& frame, size_t bytes) {
    Frame::size_type offset = frame.payload.size();
    frame.payload.resize(bytes + offset);
    char* data = reinterpret_cast<char*>(frame.payload.data()) + offset;

    size_t total = 0;

    while (total < bytes) {
        auto nr = cb::net::recv(sock, data + total, bytes - total, 0);
        if (nr <= 0) {
            auto error = cb::net::get_socket_error();
            if (nr == 0) {
                // nr == 0 means that the other end closed the connection.
                // Given that we expected to read more data, let's throw
                // an connection reset exception
                error = ECONNRESET;
            }

            throw std::system_error(error, std::system_category(),
                                    "MemcachedConnection::readPlain: failed to read data");
        } else {
            total += nr;
        }
    }
}

void MemcachedConnection::sendFrame(const Frame& frame) {
    if (ssl) {
        sendFrameSsl(frame);
    } else {
        sendFramePlain(frame);
    }
    if (packet_dump) {
        cb::mcbp::dump(frame.payload.data(), std::cerr);
    }
}

void MemcachedConnection::sendBuffer(cb::const_byte_buffer& buf) {
    iovec iov;
    iov.iov_base = const_cast<uint8_t*>(buf.data());
    iov.iov_len = buf.size();
    std::vector<iovec> list(1, iov);
    sendBuffer(list);
}

void MemcachedConnection::sendBuffer(const std::vector<iovec>& list) {
    if (ssl) {
        sendBufferSsl(list);
    } else {
        sendBufferPlain(list);
    }
}

void MemcachedConnection::sendPartialFrame(Frame& frame,
                                           Frame::size_type length) {
    // Move the remainder to a new frame.
    auto rem_first = frame.payload.begin() + length;
    auto rem_last = frame.payload.end();
    std::vector<uint8_t> remainder;
    std::copy(rem_first, rem_last, std::back_inserter(remainder));
    frame.payload.erase(rem_first, rem_last);

    // Send the partial frame.
    sendFrame(frame);

    // Swap the old payload with the remainder.
    frame.payload.swap(remainder);
}

void MemcachedConnection::read(Frame& frame, size_t bytes) {
    if (ssl) {
        readSsl(frame, bytes);
    } else {
        readPlain(frame, bytes);
    }
}

unique_cJSON_ptr MemcachedConnection::stats(const std::string& subcommand) {
    unique_cJSON_ptr ret(cJSON_CreateObject());

    for (auto& pair : statsMap(subcommand)) {
        const std::string& key = pair.first;
        const std::string& value = pair.second;
        if (value == "false") {
            cJSON_AddFalseToObject(ret.get(), key.c_str());
        } else if (value == "true") {
            cJSON_AddTrueToObject(ret.get(), key.c_str());
        } else {
            try {
                int64_t val = std::stoll(value);
                cJSON_AddNumberToObject(ret.get(), key.c_str(), val);
            } catch (...) {
                cJSON_AddStringToObject(ret.get(), key.c_str(), value.c_str());
            }
        }
    }
    return ret;
}

void MemcachedConnection::setSslCertFile(const std::string& file)  {
    if (file.empty()) {
        ssl_cert_file.clear();
        return;
    }
    auto path = file;
    cb::io::sanitizePath(path);
    if (!cb::io::isFile(path)) {
        throw std::system_error(std::make_error_code(std::errc::no_such_file_or_directory),
                                "Can't use [" + path + "]");
    }
    ssl_cert_file = path;
}

void MemcachedConnection::setSslKeyFile(const std::string& file) {
    if (file.empty()) {
        ssl_key_file.clear();
        return;
    }
    auto path = file;
    cb::io::sanitizePath(path);
    if (!cb::io::isFile(path)) {
        throw std::system_error(std::make_error_code(std::errc::no_such_file_or_directory),
                                "Can't use [" + path + "]");
    }
    ssl_key_file = path;
}

static Frame to_frame(const BinprotCommand& command) {
    Frame frame;
    command.encode(frame.payload);
    return frame;
}

std::unique_ptr<MemcachedConnection> MemcachedConnection::clone() {
    auto* result = new MemcachedConnection(
            this->host, this->port, this->family, this->ssl);
    result->setSslCertFile(this->ssl_cert_file);
    result->setSslKeyFile(this->ssl_key_file);
    result->connect();
    result->applyFeatures("", this->effective_features);
    return std::unique_ptr<MemcachedConnection>{result};
}

void MemcachedConnection::recvFrame(Frame& frame) {
    frame.reset();
    // A memcached packet starts with a 24 byte fixed header
    MemcachedConnection::read(frame, 24);

    // Following the header is the full payload specified in the field
    // bodylen. Luckily for us the bodylen is located at the same offset in
    // both a request and a response message..
    auto* req = reinterpret_cast<protocol_binary_request_header*>(
            frame.payload.data());
    const uint32_t bodylen = ntohl(req->request.bodylen);
    auto magic = cb::mcbp::Magic(frame.payload.at(0));

    if (magic != cb::mcbp::Magic::ClientRequest &&
        magic != cb::mcbp::Magic::ClientResponse &&
        magic != cb::mcbp::Magic::ServerRequest &&
        magic != cb::mcbp::Magic::ServerResponse &&
        magic != cb::mcbp::Magic::AltClientResponse) {
        throw std::runtime_error("Invalid magic received: " +
                                 std::to_string(frame.payload.at(0)));
    }

    MemcachedConnection::read(frame, bodylen);
    if (packet_dump) {
        cb::mcbp::dump(frame.payload.data(), std::cerr);
    }
}

void MemcachedConnection::sendCommand(const BinprotCommand& command) {
    traceData.reset();

    auto encoded = command.encode();

    // encoded contains the message header (as owning vector<uint8_t>),
    // plus a variable number of (non-owning) byte buffers. Create
    // a single vector of byte buffers for all; then send in a single
    // sendmsg() call (to avoid copying any data), with a single syscall.

    // Perf: this function previously used multiple calls to
    // sendBuffer() (one per header / buffer) to send the data without
    // copying / re-forming it. While this does reduce copying cost; it requires
    // one send() syscall per chunk. Benchmarks show that is actually
    // *more* expensive overall (particulary when measuring server
    // performance) as the server can read the first header chunk;
    // then attempts to read the body which hasn't been delievered yet
    // and hence has to go around the libevent loop again to read the
    // body.

    std::vector<iovec> message;
    iovec iov;
    iov.iov_base = encoded.header.data();
    iov.iov_len = encoded.header.size();
    message.push_back(iov);
    for (auto buf : encoded.bufs) {
        iov.iov_base = const_cast<uint8_t*>(buf.data());
        iov.iov_len = buf.size();
        message.push_back(iov);
    }

    sendBuffer(message);
}

void MemcachedConnection::recvResponse(BinprotResponse& response) {
    Frame frame;
    traceData.reset();
    recvFrame(frame);
    response.assign(std::move(frame.payload));
    traceData = response.getTracingData();
}

void MemcachedConnection::authenticate(const std::string& username,
                                       const std::string& password,
                                       const std::string& mech) {
    cb::sasl::client::ClientContext client(
            [username]() -> std::string { return username; },
            [password]() -> std::string { return password; },
            mech);
    auto client_data = client.start();

    if (client_data.first != cb::sasl::Error::OK) {
        throw std::runtime_error(std::string("cbsasl_client_start (") +
                                 std::string(client.getName()) +
                                 std::string("): ") +
                                 ::to_string(client_data.first));
    }

    BinprotSaslAuthCommand authCommand;
    authCommand.setChallenge(client_data.second);
    authCommand.setMechanism(client.getName());
    sendCommand(authCommand);

    BinprotResponse response;
    recvResponse(response);

    while (response.getStatus() == cb::mcbp::Status::AuthContinue) {
        auto respdata = response.getData();
        client_data =
                client.step({reinterpret_cast<const char*>(respdata.data()),
                             respdata.size()});
        if (client_data.first != cb::sasl::Error::OK &&
            client_data.first != cb::sasl::Error::CONTINUE) {
            reconnect();
            throw std::runtime_error(std::string("cbsasl_client_step: ") +
                                     ::to_string(client_data.first));
        }

        BinprotSaslStepCommand stepCommand;
        stepCommand.setMechanism(client.getName());
        stepCommand.setChallenge(client_data.second);
        sendCommand(stepCommand);
        recvResponse(response);
    }

    if (!response.isSuccess()) {
        throw ConnectionError("Authentication failed", response);
    }
}

void MemcachedConnection::createBucket(const std::string& name,
                                       const std::string& config,
                                       const BucketType type) {
    std::string module;
    switch (type) {
    case BucketType::Memcached:
        module.assign("default_engine.so");
        break;
    case BucketType::EWouldBlock:
        module.assign("ewouldblock_engine.so");
        break;
    case BucketType::Couchbase:
        module.assign("ep.so");
        break;
    default:
        throw std::runtime_error("Not implemented");
    }

    BinprotCreateBucketCommand command(name.c_str());
    command.setConfig(module, config);
    sendCommand(command);

    BinprotResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw ConnectionError("Create bucket failed", response);
    }
}

void MemcachedConnection::deleteBucket(const std::string& name) {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_DELETE_BUCKET, name);
    sendCommand(command);
    BinprotResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw ConnectionError("Delete bucket failed", response);
    }
}

void MemcachedConnection::selectBucket(const std::string& name) {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_SELECT_BUCKET, name);
    sendCommand(command);
    BinprotResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw ConnectionError(
                std::string("Select bucket [" + name + "] failed").c_str(),
                response);
    }
}

std::string MemcachedConnection::to_string() {
    std::string ret("Memcached connection ");
    ret.append(std::to_string(port));
    if (family == AF_INET6) {
        ret.append("[::1]:");
    } else {
        ret.append("127.0.0.1:");
    }

    ret.append(std::to_string(port));

    if (ssl) {
        ret.append(" ssl");
    }

    return ret;
}

std::vector<std::string> MemcachedConnection::listBuckets() {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_LIST_BUCKETS);
    sendCommand(command);

    BinprotResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw ConnectionError("List bucket failed", response);
    }

    std::vector<std::string> ret;

    // the value contains a list of bucket names separated by space.
    std::istringstream iss(response.getDataString());
    std::copy(std::istream_iterator<std::string>(iss),
              std::istream_iterator<std::string>(),
              std::back_inserter(ret));

    return ret;
}

Document MemcachedConnection::get(const std::string& id, Vbid vbucket) {
    BinprotGetCommand command;
    command.setKey(id);
    command.setVBucket(vbucket);
    sendCommand(command);

    BinprotGetResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw ConnectionError("Failed to get: " + id, response.getStatus());
    }

    Document ret;
    ret.info.flags = response.getDocumentFlags();
    ret.info.cas = response.getCas();
    ret.info.id = id;
    ret.info.datatype = cb::mcbp::Datatype(response.getDatatype());
    ret.value.assign(response.getData().data(),
                     response.getData().data() + response.getData().size());
    return ret;
}

Frame MemcachedConnection::encodeCmdGet(const std::string& id, Vbid vbucket) {
    BinprotGetCommand command;
    command.setKey(id);
    command.setVBucket(vbucket);
    return to_frame(command);
}

MutationInfo MemcachedConnection::mutate(const DocumentInfo& info,
                                         Vbid vbucket,
                                         cb::const_byte_buffer value,
                                         MutationType type) {
    BinprotMutationCommand command;
    command.setDocumentInfo(info);
    command.addValueBuffer(value);
    command.setVBucket(vbucket);
    command.setMutationType(type);
    sendCommand(command);

    BinprotMutationResponse response;
    recvResponse(response);
    if (!response.isSuccess()) {
        throw ConnectionError("Failed to store " + info.id,
                              response.getStatus());
    }

    return response.getMutationInfo();
}

MutationInfo MemcachedConnection::store(const std::string& id,
                                        Vbid vbucket,
                                        std::string value,
                                        cb::mcbp::Datatype datatype) {
    Document doc{};
    doc.value = std::move(value);
    doc.info.id = id;
    doc.info.datatype = datatype;
    return mutate(doc, vbucket, MutationType::Set);
}

std::map<std::string, std::string> MemcachedConnection::statsMap(
        const std::string& subcommand) {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_STAT, subcommand);
    sendCommand(command);

    std::map<std::string, std::string> ret;
    int counter = 0;

    while (true) {
        BinprotResponse response;
        recvResponse(response);

        if (!response.isSuccess()) {
            throw ConnectionError("Stats failed", response);
        }

        if (!response.getBodylen()) {
            break;
        }

        std::string key = response.getKeyString();

        if (key.empty()) {
            key = std::to_string(counter++);
        }
        ret.insert(std::make_pair(key, response.getDataString()));
    }

    return ret;
}

void MemcachedConnection::configureEwouldBlockEngine(const EWBEngineMode& mode,
                                                     ENGINE_ERROR_CODE err_code,
                                                     uint32_t value,
                                                     const std::string& key) {
    request_ewouldblock_ctl request;
    memset(request.bytes, 0, sizeof(request.bytes));
    request.message.header.request.magic = 0x80;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_EWOULDBLOCK_CTL;
    request.message.header.request.extlen = 12;
    request.message.header.request.keylen = ntohs((short)key.size());
    request.message.header.request.bodylen =
            htonl(12 + gsl::narrow<uint32_t>(key.size()));
    request.message.body.inject_error = htonl(err_code);
    request.message.body.mode = htonl(static_cast<uint32_t>(mode));
    request.message.body.value = htonl(value);

    Frame frame;
    frame.payload.resize(sizeof(request.bytes) + key.size());
    memcpy(frame.payload.data(), request.bytes, sizeof(request.bytes));
    memcpy(frame.payload.data() + sizeof(request.bytes),
           key.data(),
           key.size());
    sendFrame(frame);

    recvFrame(frame);
    auto* bytes = frame.payload.data();
    auto* rsp = reinterpret_cast<protocol_binary_response_no_extras*>(bytes);
    auto& header = rsp->message.header.response;
    if (header.getStatus() != cb::mcbp::Status::Success) {
        throw ConnectionError("Failed to configure ewouldblock engine",
                              header.getStatus());
    }
}

void MemcachedConnection::reloadAuditConfiguration() {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD);
    sendCommand(command);
    BinprotResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw ConnectionError("Failed to reload audit configuration", response);
    }
}

void MemcachedConnection::hello(const std::string& userAgent,
                                const std::string& userAgentVersion,
                                const std::string& comment) {
    applyFeatures(userAgent + " " + userAgentVersion, effective_features);
}

void MemcachedConnection::applyFeatures(const std::string& agent,
                                        const Featureset& featureset) {
    BinprotHelloCommand command(agent);
    for (const auto& feature : featureset) {
        command.enableFeature(cb::mcbp::Feature(feature), true);
    }

    sendCommand(command);

    BinprotHelloResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw ConnectionError("Failed to say hello", response);
    }

    effective_features.clear();
    for (const auto& feature : response.getFeatures()) {
        effective_features.insert(uint16_t(feature));
    }
}

void MemcachedConnection::setFeatures(
        const std::string& agent,
        const std::vector<cb::mcbp::Feature>& features) {
    BinprotHelloCommand command(agent);
    for (const auto& feature : features) {
        command.enableFeature(cb::mcbp::Feature(feature), true);
    }

    sendCommand(command);

    BinprotHelloResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw ConnectionError("Failed to say hello", response);
    }

    effective_features.clear();
    for (const auto& feature : response.getFeatures()) {
        effective_features.insert(uint16_t(feature));
    }

    // Verify that I was able to set all of them
    std::stringstream ss;
    ss << "[";

    for (const auto& feature : features) {
        if (!hasFeature(feature)) {
            ss << ::to_string(feature) << ",";
        }
    }

    auto missing = ss.str();
    if (missing.size() > 1) {
        missing.back() = ']';
        throw std::runtime_error("Failed to enable: " + missing);
    }
}

void MemcachedConnection::setFeature(cb::mcbp::Feature feature, bool enabled) {
    Featureset currFeatures = effective_features;
    if (enabled) {
        currFeatures.insert(uint16_t(feature));
    } else {
        currFeatures.erase(uint16_t(feature));
    }

    applyFeatures("mcbp", currFeatures);

    if (enabled && !hasFeature(feature)) {
        throw std::runtime_error("Failed to enable " + ::to_string(feature));
    } else if (!enabled && hasFeature(feature)) {
        throw std::runtime_error("Failed to disable " + ::to_string(feature));
    }
}

std::string MemcachedConnection::getSaslMechanisms() {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_SASL_LIST_MECHS);
    sendCommand(command);

    BinprotResponse response;
    recvResponse(response);
    if (!response.isSuccess()) {
        throw ConnectionError("Failed to fetch sasl mechanisms", response);
    }

    return response.getDataString();
}

std::string MemcachedConnection::ioctl_get(const std::string& key) {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_IOCTL_GET, key);
    sendCommand(command);

    BinprotResponse response;
    recvResponse(response);
    if (!response.isSuccess()) {
        throw ConnectionError("ioctl_get '" + key + "' failed", response);
    }
    return std::string(reinterpret_cast<const char*>(response.getPayload()),
                       response.getBodylen());
}

void MemcachedConnection::ioctl_set(const std::string& key,
                                    const std::string& value) {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_IOCTL_SET, key, value);
    sendCommand(command);

    BinprotResponse response;
    recvResponse(response);
    if (!response.isSuccess()) {
        throw ConnectionError("ioctl_set '" + key + "' failed", response);
    }
}

uint64_t MemcachedConnection::increment(const std::string& key,
                                        uint64_t delta,
                                        uint64_t initial,
                                        rel_time_t exptime,
                                        MutationInfo* info) {
    return incr_decr(
            PROTOCOL_BINARY_CMD_INCREMENT, key, delta, initial, exptime, info);
}

uint64_t MemcachedConnection::decrement(const std::string& key,
                                        uint64_t delta,
                                        uint64_t initial,
                                        rel_time_t exptime,
                                        MutationInfo* info) {
    return incr_decr(
            PROTOCOL_BINARY_CMD_DECREMENT, key, delta, initial, exptime, info);
}

uint64_t MemcachedConnection::incr_decr(protocol_binary_command opcode,
                                        const std::string& key,
                                        uint64_t delta,
                                        uint64_t initial,
                                        rel_time_t exptime,
                                        MutationInfo* info) {
    const char* opcode_name =
            (opcode == PROTOCOL_BINARY_CMD_INCREMENT) ? "incr" : "decr";

    BinprotIncrDecrCommand command;
    command.setOp(opcode).setKey(key);
    command.setDelta(delta).setInitialValue(initial).setExpiry(exptime);

    sendCommand(command);

    BinprotIncrDecrResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw ConnectionError(
                std::string(opcode_name) + " \"" + key + "\" failed.",
                response.getStatus());
    }

    if (response.getDatatype() != PROTOCOL_BINARY_RAW_BYTES) {
        throw ValidationError(
                std::string(opcode_name) + " \"" + key +
                "\"invalid - response has incorrect datatype (" +
                mcbp::datatype::to_string(response.getDatatype()) + ")");
    }

    if (info != nullptr) {
        *info = response.getMutationInfo();
    }
    return response.getValue();
}

MutationInfo MemcachedConnection::remove(const std::string& key,
                                         Vbid vbucket,
                                         uint64_t cas) {
    BinprotRemoveCommand command;
    command.setKey(key).setVBucket(vbucket);
    command.setVBucket(vbucket);
    command.setCas(cas);
    sendCommand(command);

    BinprotRemoveResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw ConnectionError("Failed to remove: " + key, response.getStatus());
    }

    return response.getMutationInfo();
}

Document MemcachedConnection::get_and_lock(const std::string& id,
                                           Vbid vbucket,
                                           uint32_t lock_timeout) {
    BinprotGetAndLockCommand command;
    command.setKey(id);
    command.setVBucket(vbucket);
    command.setLockTimeout(lock_timeout);
    sendCommand(command);

    BinprotGetAndLockResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw ConnectionError("Failed to get: " + id, response.getStatus());
    }

    Document ret;
    ret.info.flags = response.getDocumentFlags();
    ret.info.cas = response.getCas();
    ret.info.id = id;
    ret.info.datatype = cb::mcbp::Datatype(response.getDatatype());
    ret.value.assign(response.getData().data(),
                     response.getData().data() + response.getData().size());
    return ret;
}

BinprotResponse MemcachedConnection::getFailoverLog(Vbid vbucket) {
    BinprotGetFailoverLogCommand command;
    command.setVBucket(vbucket);
    sendCommand(command);

    BinprotResponse response;
    recvResponse(response);

    return response;
}

void MemcachedConnection::unlock(const std::string& id,
                                 Vbid vbucket,
                                 uint64_t cas) {
    BinprotUnlockCommand command;
    command.setKey(id);
    command.setVBucket(vbucket);
    command.setCas(cas);
    sendCommand(command);

    BinprotUnlockResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw ConnectionError("unlock(): " + id, response.getStatus());
    }
}

void MemcachedConnection::dropPrivilege(cb::rbac::Privilege privilege) {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_DROP_PRIVILEGE,
                                  cb::rbac::to_string(privilege));
    sendCommand(command);

    BinprotResponse response;
    recvResponse(response);
    if (!response.isSuccess()) {
        throw ConnectionError("dropPrivilege \"" +
                                      cb::rbac::to_string(privilege) +
                                      "\" failed.",
                              response.getStatus());
    }
}

MutationInfo MemcachedConnection::mutateWithMeta(
        Document& doc,
        Vbid vbucket,
        uint64_t cas,
        uint64_t seqno,
        uint32_t metaOption,
        std::vector<uint8_t> metaExtras) {
    BinprotSetWithMetaCommand swm(
            doc, vbucket, cas, seqno, metaOption, metaExtras);
    sendCommand(swm);

    BinprotMutationResponse response;
    recvResponse(response);
    if (!response.isSuccess()) {
        throw ConnectionError("Failed to mutateWithMeta " + doc.info.id,
                              response.getStatus());
    }

    return response.getMutationInfo();
}

ObserveInfo MemcachedConnection::observeSeqno(Vbid vbid, uint64_t uuid) {
    BinprotObserveSeqnoCommand observe(vbid, uuid);
    sendCommand(observe);

    BinprotObserveSeqnoResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw ConnectionError(std::string("Failed to observeSeqno for ") +
                                      vbid.to_string() + " uuid:" +
                                      std::to_string(uuid),
                              response.getStatus());
    }
    return response.info;
}

void MemcachedConnection::enablePersistence() {
    sendCommand(BinprotGenericCommand(PROTOCOL_BINARY_CMD_START_PERSISTENCE));

    BinprotResponse response;
    recvResponse(response);
    if (!response.isSuccess()) {
        throw ConnectionError("Failed to enablePersistence ",
                              response.getStatus());
    }
}

void MemcachedConnection::disablePersistence() {
    sendCommand(BinprotGenericCommand(PROTOCOL_BINARY_CMD_STOP_PERSISTENCE));

    BinprotResponse response;
    recvResponse(response);
    if (!response.isSuccess()) {
        throw ConnectionError("Failed to disablePersistence ",
                              response.getStatus());
    }
}

std::pair<cb::mcbp::Status, GetMetaResponse> MemcachedConnection::getMeta(
        const std::string& key, Vbid vbucket, GetMetaVersion version) {
    BinprotGenericCommand cmd{PROTOCOL_BINARY_CMD_GET_META, key};
    const std::vector<uint8_t> extras = {uint8_t(version)};
    cmd.setExtras(extras);
    sendCommand(cmd);
    BinprotResponse resp;
    recvResponse(resp);

    GetMetaResponse meta;
    memcpy(&meta, resp.getPayload(), resp.getBodylen());
    meta.deleted = ntohl(meta.deleted);
    meta.expiry = ntohl(meta.expiry);
    meta.seqno = ntohll(meta.seqno);

    return std::make_pair(resp.getStatus(), meta);
}

void MemcachedConnection::setUnorderedExecutionMode(ExecutionMode mode) {
    switch (mode) {
    case ExecutionMode::Ordered:
        setFeature(cb::mcbp::Feature::UnorderedExecution, false);
        return;
    case ExecutionMode::Unordered:
        setFeature(cb::mcbp::Feature::UnorderedExecution, true);
        return;
    }
    throw std::invalid_argument("setUnorderedExecutionMode: Invalid mode");
}

BinprotResponse MemcachedConnection::execute(const BinprotCommand &command) {
    BinprotResponse response;
    executeCommand(command, response);
    return response;
}

/////////////////////////////////////////////////////////////////////////
// Implementation of the ConnectionError class
/////////////////////////////////////////////////////////////////////////

// Generates error msgs like ``<prefix>: ["<context>", ]<reason> (#<reason>)``
static std::string formatMcbpExceptionMsg(const std::string& prefix,
                                          cb::mcbp::Status reason,
                                          const std::string& context = "") {
    // Format the error message
    std::string errormessage(prefix);
    errormessage.append(": ");

    if (!context.empty()) {
        errormessage.append("'");
        errormessage.append(context);
        errormessage.append("', ");
    }

    errormessage.append(to_string(reason));
    errormessage.append(" (");
    errormessage.append(std::to_string(uint16_t(reason)));
    errormessage.append(")");
    return errormessage;
}

static std::string formatMcbpExceptionMsg(const std::string& prefix,
                                          const BinprotResponse& response) {
    std::string context;
    // If the response was not a success and the datatype is json then there's
    // probably a JSON error context that's been included with the response body
    if (mcbp::datatype::is_json(response.getDatatype()) &&
        !response.isSuccess()) {
        unique_cJSON_ptr json =
                unique_cJSON_ptr(cJSON_Parse(response.getDataString().c_str()));
        if (json != nullptr && json->type == cJSON_Object) {
            auto* error = cJSON_GetObjectItem(json.get(), "error");
            if (error != nullptr && error->type == cJSON_Object) {
                auto* ctx = cJSON_GetObjectItem(error, "context");
                if (ctx != nullptr && ctx->type == cJSON_String) {
                    context = ctx->valuestring;
                }
            }
        }
    }
    return formatMcbpExceptionMsg(prefix, response.getStatus(), context);
}

ConnectionError::ConnectionError(const std::string& prefix,
                                 cb::mcbp::Status reason)
    : std::runtime_error(formatMcbpExceptionMsg(prefix, reason).c_str()),
      reason(reason) {
}

ConnectionError::ConnectionError(const std::string& prefix,
                                 const BinprotResponse& response)
    : std::runtime_error(formatMcbpExceptionMsg(prefix, response).c_str()),
      reason(response.getStatus()),
      payload(response.getDataString()) {
}

std::string ConnectionError::getErrorReference() const {
    const auto decoded = nlohmann::json::parse(payload);
    return decoded["error"]["ref"];
}

std::string ConnectionError::getErrorContext() const {
    const auto decoded = nlohmann::json::parse(payload);
    return decoded["error"]["context"];
}

std::string ConnectionError::getPayload() const {
    return payload;
}

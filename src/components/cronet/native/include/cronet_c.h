// Copyright 2017 The Chromium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef COMPONENTS_CRONET_NATIVE_INCLUDE_CRONET_C_H_
#define COMPONENTS_CRONET_NATIVE_INCLUDE_CRONET_C_H_

#include <stddef.h>

#include "cronet_export.h"

// Cronet public C API is generated from cronet.idl
#include "cronet.idl_c.h"

#ifdef __cplusplus
extern "C" {
#endif

// Stream Engine used by Bidirectional Stream C API for GRPC.
typedef struct stream_engine stream_engine;

// Additional Cronet C API not generated from cronet.idl.

// Sets net::CertVerifier* raw_mock_cert_verifier for testing of Cronet_Engine.
// Must be called before Cronet_Engine_InitWithParams().
CRONET_EXPORT void Cronet_Engine_SetMockCertVerifierForTesting(
    Cronet_EnginePtr engine,
    /* net::CertVerifier* */ void* raw_mock_cert_verifier);

// Returns "stream_engine" interface for bidirectionsl stream support for GRPC.
// Returned stream engine is owned by Cronet Engine and is only valid until
// Cronet_Engine_Shutdown().
CRONET_EXPORT stream_engine* Cronet_Engine_GetStreamEngine(
    Cronet_EnginePtr engine);

// Creates a CertVerifier that uses custom root certificates for validation.
// pem_root_certs: PEM-formatted root certificates (can contain multiple certs).
// Returns a pointer to the created net::CertVerifier.
// The caller is responsible for passing it to
// Cronet_Engine_SetMockCertVerifierForTesting() which takes ownership.
// Returns nullptr if the PEM data is invalid or no valid certificates found.
CRONET_EXPORT void* Cronet_CreateCertVerifierWithRootCerts(
    const char* pem_root_certs);

// Dialer callback type for custom TCP connection establishment.
// context: User-provided context pointer passed to Cronet_Engine_SetDialer.
// address: IP address string (e.g. "1.2.3.4" or "::1").
// port: Port number.
// Returns: connected socket fd on success, negative net error code on failure.
// Common error codes:
//   -102: ERR_CONNECTION_REFUSED
//   -104: ERR_CONNECTION_FAILED
//   -109: ERR_ADDRESS_UNREACHABLE
//   -118: ERR_CONNECTION_TIMED_OUT
typedef int (*Cronet_DialerFunc)(void* context,
                                 const char* address,
                                 uint16_t port);

// Sets a custom dialer for TCP connections.
// When set, the engine will use this callback to establish TCP connections
// instead of the default system socket API.
// Must be called before Cronet_Engine_StartWithParams().
// dialer: The callback function to use for TCP connections, or nullptr to
//         disable custom dialing.
// context: User-provided context pointer that will be passed to the dialer.
CRONET_EXPORT void Cronet_Engine_SetDialer(Cronet_EnginePtr engine,
                                           Cronet_DialerFunc dialer,
                                           void* context);

// Asynchronous TCP dialer callback type.
// context: User-provided context pointer passed to Cronet_Engine_SetAsyncDialer.
// address: IP address string (e.g. "1.2.3.4" or "::1").
// port: Port number.
// dial_id: Identifies this dial in Cronet_Engine_CompleteAsyncDial().
// Runs on the engine's network thread and must return without waiting for the
// connection. Exactly one Cronet_Engine_CompleteAsyncDial(dial_id, ...) must
// follow, from any thread.
typedef void (*Cronet_AsyncDialerFunc)(void* context,
                                       const char* address,
                                       uint16_t port,
                                       uint64_t dial_id);

// Sets the asynchronous dialer for TCP connections. Takes precedence over
// Cronet_Engine_SetDialer. Must be called before
// Cronet_Engine_StartWithParams().
CRONET_EXPORT void Cronet_Engine_SetAsyncDialer(Cronet_EnginePtr engine,
                                                Cronet_AsyncDialerFunc dialer,
                                                void* context);

// Delivers the result of an asynchronous dial. Thread-safe.
// result: connected socket fd (>= 0), ownership passes to Cronet; or a
//         negative net error code. A dial that Cronet no longer waits for
//         (connect timeout, cancellation, engine shutdown) is accepted and its
//         fd is closed.
CRONET_EXPORT void Cronet_Engine_CompleteAsyncDial(uint64_t dial_id,
                                                   int result);

// UDP Dialer callback type for custom UDP socket creation.
// context: User-provided context pointer passed to Cronet_Engine_SetUdpDialer.
// address: IP address string (e.g. "1.2.3.4" or "::1").
// port: Port number.
// out_local_address: Output buffer for local IP address (caller provides buffer,
//                    should be at least 46 bytes for INET6_ADDRSTRLEN).
// out_local_port: Output pointer for local port number.
// Returns: socket fd on success, negative net error code on failure.
// The returned socket can be:
//   - AF_INET/AF_INET6 SOCK_DGRAM: Standard UDP socket
//   - AF_UNIX SOCK_DGRAM: Unix domain datagram socket (Unix/macOS/Linux)
//   - AF_UNIX SOCK_STREAM: Unix domain stream socket (Windows, with framing)
typedef int (*Cronet_UdpDialerFunc)(void* context,
                                    const char* address,
                                    uint16_t port,
                                    char* out_local_address,
                                    uint16_t* out_local_port);

// Sets a custom dialer for UDP sockets.
// When set, the engine will use this callback to create UDP sockets
// instead of the default system socket API.
// Must be called before Cronet_Engine_StartWithParams().
// dialer: The callback function to use for UDP sockets, or nullptr to
//         disable custom UDP dialing.
// context: User-provided context pointer that will be passed to the dialer.
CRONET_EXPORT void Cronet_Engine_SetUdpDialer(Cronet_EnginePtr engine,
                                              Cronet_UdpDialerFunc dialer,
                                              void* context);

// Closes all connections managed by the engine's network session.
// This includes socket pools, HTTP stream pool, SPDY session pool,
// and QUIC session pool. Useful for releasing connection-related memory
// or speeding up engine shutdown.
// Must be called after Cronet_Engine_StartWithParams().
// Returns after posting the work to the network thread; it does not wait for
// the connections to close. Work posted to the network thread afterwards runs
// after it.
CRONET_EXPORT void Cronet_Engine_CloseAllConnections(Cronet_EnginePtr engine);

// WebSocket client on an engine's URL request context. The connection uses the
// engine's socket factory, DNS, certificate verification and connection pools:
// HTTP/1.1 Upgrade, or RFC 8441 / RFC 9220 when the server's SETTINGS allow it.
// Callbacks run synchronously on the engine's network thread and must not
// block. All other functions may be called from any thread; they post to the
// network thread.
typedef struct Cronet_WebSocket Cronet_WebSocket;
typedef Cronet_WebSocket* Cronet_WebSocketPtr;

typedef struct Cronet_WebSocketHeader {
  const char* name;
  const char* value;
} Cronet_WebSocketHeader;

typedef struct Cronet_WebSocketCallbacks {
  // The opening handshake succeeded. |headers| holds ":status" followed by the
  // response header lines; valid only during the call.
  void (*on_connected)(Cronet_WebSocketPtr ws,
                       const Cronet_WebSocketHeader* headers,
                       size_t header_count);
  // Payload bytes of a received data message (text or binary; message
  // boundaries are not reported), valid only during the call. Returning 0
  // stops reading from the connection after the frames already read; reading
  // resumes on Cronet_WebSocket_ReadMore(). Returning nonzero keeps reading.
  int (*on_data)(Cronet_WebSocketPtr ws, const char* data, size_t size);
  // Every message passed to Cronet_WebSocket_Send() so far has been written
  // to the connection.
  void (*on_send_done)(Cronet_WebSocketPtr ws);
  // The connection has ended; no callback follows. |net_error| is 0 after a
  // completed closing handshake, otherwise a net error. |code| is the close
  // code of a dropped connection, or the HTTP status of a rejected opening
  // handshake (0 when there is none).
  void (*on_closed)(Cronet_WebSocketPtr ws,
                    int net_error,
                    int code,
                    const char* reason);
} Cronet_WebSocketCallbacks;

// Creates a WebSocket on a started engine. |callbacks| must stay valid until
// the WebSocket is destroyed. Returns nullptr when the engine is not running.
// The WebSocket must be destroyed before Cronet_Engine_Shutdown().
CRONET_EXPORT Cronet_WebSocketPtr
Cronet_WebSocket_Create(Cronet_EnginePtr engine,
                        const Cronet_WebSocketCallbacks* callbacks);

// Starts the opening handshake to |url| (ws:// or wss://). |origin| is the
// Origin header value; empty or nullptr uses the https origin of |url|.
// |headers| are extra request headers. Returns 0 on success, -1 for an invalid
// URL or origin, or the 1-based index of an invalid header.
CRONET_EXPORT int Cronet_WebSocket_Start(Cronet_WebSocketPtr ws,
                                         const char* url,
                                         const char* origin,
                                         const Cronet_WebSocketHeader* headers,
                                         size_t header_count);

// Sends |data| as one binary message; the bytes are copied. Only valid after
// on_connected. Ignored once closing has started.
CRONET_EXPORT void Cronet_WebSocket_Send(Cronet_WebSocketPtr ws,
                                         const char* data,
                                         size_t size);

// Resumes reading after on_data returned 0.
CRONET_EXPORT void Cronet_WebSocket_ReadMore(Cronet_WebSocketPtr ws);

// Starts the closing handshake with |code| (1000-4999) and |reason|; before
// the opening handshake completes, abandons it. on_closed follows.
CRONET_EXPORT void Cronet_WebSocket_Close(Cronet_WebSocketPtr ws,
                                          int code,
                                          const char* reason);

// Destroys the WebSocket, dropping the connection without a closing handshake
// if it is still open. Callbacks already posted may still run until the
// destruction task runs on the network thread; none run afterwards.
CRONET_EXPORT void Cronet_WebSocket_Destroy(Cronet_WebSocketPtr ws);

#ifdef __cplusplus
}
#endif

#endif  // COMPONENTS_CRONET_NATIVE_INCLUDE_CRONET_C_H_

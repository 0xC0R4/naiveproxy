// Copyright 2026 The Chromium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Cronet_WebSocket_*: a net::WebSocketChannel on the engine's
// URLRequestContext (see cronet_c.h). The threading follows the grpc_support
// bidirectional stream: the C callbacks run on the network thread, every C
// call posts to it, and Destroy deletes the object there.

#include <stdint.h>

#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "base/check.h"
#include "base/compiler_specific.h"
#include "base/containers/span.h"
#include "base/functional/bind.h"
#include "base/location.h"
#include "base/memory/raw_ptr.h"
#include "base/memory/scoped_refptr.h"
#include "base/memory/weak_ptr.h"
#include "base/strings/string_number_conversions.h"
#include "base/task/single_thread_task_runner.h"
#include "components/cronet/native/engine.h"
#include "components/cronet/native/include/cronet_c.h"
#include "components/grpc_support/include/bidirectional_stream_c.h"
#include "net/base/auth.h"
#include "net/base/io_buffer.h"
#include "net/base/isolation_info.h"
#include "net/base/net_errors.h"
#include "net/http/http_request_headers.h"
#include "net/http/http_response_headers.h"
#include "net/http/http_util.h"
#include "net/storage_access_api/status.h"
#include "net/traffic_annotation/network_traffic_annotation.h"
#include "net/url_request/url_request_context_getter.h"
#include "net/websockets/websocket_channel.h"
#include "net/websockets/websocket_errors.h"
#include "net/websockets/websocket_event_interface.h"
#include "net/websockets/websocket_frame.h"
#include "net/websockets/websocket_handshake_response_info.h"
#include "net/websockets/websocket_stream.h"
#include "url/gurl.h"
#include "url/origin.h"

struct Cronet_WebSocket {};

namespace {

class WebSocketImpl;

// Owned by the WebSocketChannel; forwards to the WebSocketImpl that owns the
// channel.
class EventInterface final : public net::WebSocketEventInterface {
 public:
  explicit EventInterface(WebSocketImpl* owner);
  ~EventInterface() override;

  void OnCreateURLRequest(net::URLRequest* request) override {}
  int OnURLRequestConnected(net::URLRequest* request,
                            const net::TransportInfo& info,
                            net::CompletionOnceCallback callback) override;
  void OnAddChannelResponse(
      std::unique_ptr<net::WebSocketHandshakeResponseInfo> response,
      const std::string& selected_subprotocol,
      const std::string& extensions) override;
  void OnDataFrame(bool fin,
                   WebSocketMessageType type,
                   base::span<const char> payload) override;
  bool HasPendingDataFrames() override;
  void OnSendDataFrameDone() override;
  void OnClosingHandshake() override {}
  void OnDropChannel(bool was_clean,
                     uint16_t code,
                     const std::string& reason) override;
  void OnFailChannel(const std::string& message,
                     int net_error,
                     std::optional<int> response_code) override;
  void OnStartOpeningHandshake(
      std::unique_ptr<net::WebSocketHandshakeRequestInfo> request) override {}
  void OnSSLCertificateError(
      std::unique_ptr<SSLErrorCallbacks> ssl_error_callbacks,
      const GURL& url,
      int net_error,
      const net::SSLInfo& ssl_info,
      bool fatal) override;
  int OnAuthRequired(
      const net::AuthChallengeInfo& auth_info,
      scoped_refptr<net::HttpResponseHeaders> response_headers,
      const net::IPEndPoint& socket_address,
      base::OnceCallback<void(const net::AuthCredentials*)> callback,
      std::optional<net::AuthCredentials>* credentials) override;

 private:
  raw_ptr<WebSocketImpl> owner_;
};

class WebSocketImpl : public Cronet_WebSocket {
 public:
  WebSocketImpl(net::URLRequestContextGetter* context_getter,
                const Cronet_WebSocketCallbacks* callbacks);
  WebSocketImpl(const WebSocketImpl&) = delete;
  WebSocketImpl& operator=(const WebSocketImpl&) = delete;

  static WebSocketImpl* From(Cronet_WebSocketPtr ws) {
    return static_cast<WebSocketImpl*>(ws);
  }

  // Any thread.
  int Start(const char* url,
            const char* origin,
            const Cronet_WebSocketHeader* headers,
            size_t header_count);
  void Send(const char* data, size_t size);
  void ReadMore();
  void Close(int code, const char* reason);
  void Destroy();

  // Network thread, from EventInterface.
  void OnConnected(const net::WebSocketHandshakeResponseInfo& response);
  void OnData(base::span<const char> payload);
  bool pending() const { return pending_; }
  void OnSendDone();
  void OnFinished(int net_error, int code, const std::string& reason);

 private:
  ~WebSocketImpl();

  void StartOnNetworkThread(GURL url,
                            url::Origin origin,
                            net::HttpRequestHeaders headers);
  void SendOnNetworkThread(scoped_refptr<net::IOBuffer> buffer, size_t size);
  void ReadMoreOnNetworkThread();
  void CloseOnNetworkThread(int code, std::string reason);
  void DestroyOnNetworkThread();
  void DropChannel();

  const raw_ptr<net::URLRequestContextGetter> context_getter_;
  const scoped_refptr<base::SingleThreadTaskRunner> network_task_runner_;
  const Cronet_WebSocketCallbacks callbacks_;

  // Network thread only.
  std::unique_ptr<net::WebSocketChannel> channel_;
  bool connected_ = false;
  bool finished_ = false;
  bool pending_ = false;

  base::WeakPtrFactory<WebSocketImpl> weak_factory_{this};
};

EventInterface::EventInterface(WebSocketImpl* owner) : owner_(owner) {}

EventInterface::~EventInterface() = default;

int EventInterface::OnURLRequestConnected(net::URLRequest* request,
                                          const net::TransportInfo& info,
                                          net::CompletionOnceCallback callback) {
  return net::OK;
}

void EventInterface::OnAddChannelResponse(
    std::unique_ptr<net::WebSocketHandshakeResponseInfo> response,
    const std::string& selected_subprotocol,
    const std::string& extensions) {
  owner_->OnConnected(*response);
}

void EventInterface::OnDataFrame(bool fin,
                                 WebSocketMessageType type,
                                 base::span<const char> payload) {
  owner_->OnData(payload);
}

bool EventInterface::HasPendingDataFrames() {
  return owner_->pending();
}

void EventInterface::OnSendDataFrameDone() {
  owner_->OnSendDone();
}

void EventInterface::OnDropChannel(bool was_clean,
                                   uint16_t code,
                                   const std::string& reason) {
  owner_->OnFinished(was_clean ? net::OK : net::ERR_CONNECTION_CLOSED, code,
                     reason);
}

void EventInterface::OnFailChannel(const std::string& message,
                                   int net_error,
                                   std::optional<int> response_code) {
  owner_->OnFinished(net_error == net::OK ? net::ERR_FAILED : net_error,
                     response_code.value_or(0), message);
}

void EventInterface::OnSSLCertificateError(
    std::unique_ptr<SSLErrorCallbacks> ssl_error_callbacks,
    const GURL& url,
    int net_error,
    const net::SSLInfo& ssl_info,
    bool fatal) {
  ssl_error_callbacks->CancelSSLRequest(net_error, &ssl_info);
}

int EventInterface::OnAuthRequired(
    const net::AuthChallengeInfo& auth_info,
    scoped_refptr<net::HttpResponseHeaders> response_headers,
    const net::IPEndPoint& socket_address,
    base::OnceCallback<void(const net::AuthCredentials*)> callback,
    std::optional<net::AuthCredentials>* credentials) {
  *credentials = std::nullopt;
  return net::OK;
}

WebSocketImpl::WebSocketImpl(net::URLRequestContextGetter* context_getter,
                             const Cronet_WebSocketCallbacks* callbacks)
    : context_getter_(context_getter),
      network_task_runner_(context_getter->GetNetworkTaskRunner()),
      callbacks_(*callbacks) {}

WebSocketImpl::~WebSocketImpl() = default;

int WebSocketImpl::Start(const char* url,
                         const char* origin,
                         const Cronet_WebSocketHeader* headers,
                         size_t header_count) {
  GURL socket_url(url ? url : "");
  if (!socket_url.is_valid() || !socket_url.SchemeIsWSOrWSS()) {
    return -1;
  }
  url::Origin request_origin;
  if (origin && *origin) {
    GURL origin_url(origin);
    if (!origin_url.is_valid()) {
      return -1;
    }
    request_origin = url::Origin::Create(origin_url);
  } else {
    GURL::Replacements replacements;
    replacements.SetSchemeStr("https");
    request_origin =
        url::Origin::Create(socket_url.ReplaceComponents(replacements));
  }
  net::HttpRequestHeaders request_headers;
  // SAFETY: the caller passes |header_count| entries.
  auto header_span =
      headers ? UNSAFE_BUFFERS(base::span(headers, header_count))
              : base::span<const Cronet_WebSocketHeader>();
  for (size_t i = 0; i < header_span.size(); ++i) {
    std::string name(header_span[i].name ? header_span[i].name : "");
    std::string value(header_span[i].value ? header_span[i].value : "");
    if (!net::HttpUtil::IsValidHeaderName(name) ||
        !net::HttpUtil::IsValidHeaderValue(value)) {
      return static_cast<int>(i) + 1;
    }
    request_headers.SetHeader(name, value);
  }
  network_task_runner_->PostTask(
      FROM_HERE,
      base::BindOnce(&WebSocketImpl::StartOnNetworkThread,
                     base::Unretained(this), std::move(socket_url),
                     std::move(request_origin), std::move(request_headers)));
  return 0;
}

void WebSocketImpl::StartOnNetworkThread(GURL url,
                                         url::Origin origin,
                                         net::HttpRequestHeaders headers) {
  DCHECK(network_task_runner_->BelongsToCurrentThread());
  if (channel_ || finished_) {
    return;
  }
  channel_ = std::make_unique<net::WebSocketChannel>(
      std::make_unique<EventInterface>(this),
      context_getter_->GetURLRequestContext());
  channel_->SendAddChannelRequest(url, std::vector<std::string>(), origin,
                                  net::StorageAccessApiStatus::kNone,
                                  net::IsolationInfo(), headers,
                                  net::WebSocketPriorityHint::kDefault,
                                  MISSING_TRAFFIC_ANNOTATION);
}

void WebSocketImpl::Send(const char* data, size_t size) {
  // SAFETY: the caller passes |size| bytes at |data|.
  auto bytes = data ? UNSAFE_BUFFERS(base::span(
                          reinterpret_cast<const uint8_t*>(data), size))
                    : base::span<const uint8_t>();
  auto buffer = base::MakeRefCounted<net::VectorIOBuffer>(bytes);
  network_task_runner_->PostTask(
      FROM_HERE,
      base::BindOnce(&WebSocketImpl::SendOnNetworkThread,
                     base::Unretained(this), std::move(buffer), bytes.size()));
}

void WebSocketImpl::SendOnNetworkThread(scoped_refptr<net::IOBuffer> buffer,
                                        size_t size) {
  DCHECK(network_task_runner_->BelongsToCurrentThread());
  if (!channel_ || !connected_ || finished_) {
    return;
  }
  // The return value only says whether OnDropChannel / OnFailChannel ran,
  // which OnFinished has handled; the channel itself is deleted later.
  std::ignore = channel_->SendFrame(
      true, net::WebSocketFrameHeader::kOpCodeBinary, std::move(buffer), size);
}

void WebSocketImpl::ReadMore() {
  network_task_runner_->PostTask(
      FROM_HERE, base::BindOnce(&WebSocketImpl::ReadMoreOnNetworkThread,
                                base::Unretained(this)));
}

void WebSocketImpl::ReadMoreOnNetworkThread() {
  DCHECK(network_task_runner_->BelongsToCurrentThread());
  pending_ = false;
  if (!channel_ || !connected_ || finished_) {
    return;
  }
  std::ignore = channel_->ReadFrames();
}

void WebSocketImpl::Close(int code, const char* reason) {
  network_task_runner_->PostTask(
      FROM_HERE,
      base::BindOnce(&WebSocketImpl::CloseOnNetworkThread,
                     base::Unretained(this), code,
                     std::string(reason ? reason : "")));
}

void WebSocketImpl::CloseOnNetworkThread(int code, std::string reason) {
  DCHECK(network_task_runner_->BelongsToCurrentThread());
  if (!channel_ || finished_) {
    return;
  }
  if (code < 1000 || code > 4999) {
    code = net::kWebSocketNormalClosure;
  }
  std::ignore =
      channel_->StartClosingHandshake(static_cast<uint16_t>(code), reason);
}

void WebSocketImpl::Destroy() {
  network_task_runner_->PostTask(
      FROM_HERE, base::BindOnce(&WebSocketImpl::DestroyOnNetworkThread,
                                base::Unretained(this)));
}

void WebSocketImpl::DestroyOnNetworkThread() {
  DCHECK(network_task_runner_->BelongsToCurrentThread());
  // Deleting the channel does not call back into the event interface.
  channel_.reset();
  delete this;
}

void WebSocketImpl::OnConnected(
    const net::WebSocketHandshakeResponseInfo& response) {
  connected_ = true;
  std::vector<std::pair<std::string, std::string>> lines;
  if (response.headers) {
    lines.emplace_back(":status",
                       base::NumberToString(response.headers->response_code()));
    size_t iter = 0;
    std::string name;
    std::string value;
    while (response.headers->EnumerateHeaderLines(&iter, &name, &value)) {
      lines.emplace_back(name, value);
    }
  }
  std::vector<Cronet_WebSocketHeader> headers;
  headers.reserve(lines.size());
  for (const auto& line : lines) {
    headers.push_back({line.first.c_str(), line.second.c_str()});
  }
  callbacks_.on_connected(this, headers.data(), headers.size());
  // Reading starts outside the channel's connect callback.
  network_task_runner_->PostTask(
      FROM_HERE, base::BindOnce(&WebSocketImpl::ReadMoreOnNetworkThread,
                                weak_factory_.GetWeakPtr()));
}

void WebSocketImpl::OnData(base::span<const char> payload) {
  if (finished_ || payload.empty()) {
    return;
  }
  if (!callbacks_.on_data(this, payload.data(), payload.size())) {
    pending_ = true;
  }
}

void WebSocketImpl::OnSendDone() {
  if (finished_) {
    return;
  }
  callbacks_.on_send_done(this);
}

void WebSocketImpl::OnFinished(int net_error,
                               int code,
                               const std::string& reason) {
  if (finished_) {
    return;
  }
  finished_ = true;
  callbacks_.on_closed(this, net_error, code, reason.c_str());
  // The channel is inside the call that reported the end; delete it after.
  network_task_runner_->PostTask(
      FROM_HERE,
      base::BindOnce(&WebSocketImpl::DropChannel, weak_factory_.GetWeakPtr()));
}

void WebSocketImpl::DropChannel() {
  channel_.reset();
}

}  // namespace

CRONET_EXPORT Cronet_WebSocketPtr
Cronet_WebSocket_Create(Cronet_EnginePtr engine,
                        const Cronet_WebSocketCallbacks* callbacks) {
  if (!engine || !callbacks || !callbacks->on_connected || !callbacks->on_data ||
      !callbacks->on_send_done || !callbacks->on_closed) {
    return nullptr;
  }
  stream_engine* stream_engine =
      static_cast<cronet::Cronet_EngineImpl*>(engine)
          ->GetBidirectionalStreamEngine();
  if (!stream_engine || !stream_engine->obj) {
    return nullptr;
  }
  return new WebSocketImpl(
      static_cast<net::URLRequestContextGetter*>(stream_engine->obj),
      callbacks);
}

CRONET_EXPORT int Cronet_WebSocket_Start(Cronet_WebSocketPtr ws,
                                         const char* url,
                                         const char* origin,
                                         const Cronet_WebSocketHeader* headers,
                                         size_t header_count) {
  return WebSocketImpl::From(ws)->Start(url, origin, headers, header_count);
}

CRONET_EXPORT void Cronet_WebSocket_Send(Cronet_WebSocketPtr ws,
                                         const char* data,
                                         size_t size) {
  WebSocketImpl::From(ws)->Send(data, size);
}

CRONET_EXPORT void Cronet_WebSocket_ReadMore(Cronet_WebSocketPtr ws) {
  WebSocketImpl::From(ws)->ReadMore();
}

CRONET_EXPORT void Cronet_WebSocket_Close(Cronet_WebSocketPtr ws,
                                          int code,
                                          const char* reason) {
  WebSocketImpl::From(ws)->Close(code, reason);
}

CRONET_EXPORT void Cronet_WebSocket_Destroy(Cronet_WebSocketPtr ws) {
  WebSocketImpl::From(ws)->Destroy();
}

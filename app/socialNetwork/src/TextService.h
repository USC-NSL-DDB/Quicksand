#pragma once

#include <future>
#include <iostream>
#include <regex>
#include <string>

#include <nu/rem_obj.hpp>

#include "../gen-cpp/TextService.h"
#include "../gen-cpp/UrlShortenService.h"
#include "../gen-cpp/UserMentionService.h"
#include "UrlShortenService.h"
#include "UserMentionService.h"

namespace social_network {

class TextService {
public:
  TextService();
  TextServiceReturn ComposeText(int64_t, std::string &&);

private:
  nu::RemObj<UrlShortenService> _url_shorten_service_obj;
  nu::RemObj<UserMentionService> _user_mention_service_obj;
};

TextService::TextService() {
  _url_shorten_service_obj = nu::RemObj<UrlShortenService>::create_pinned();
  _user_mention_service_obj = nu::RemObj<UserMentionService>::create_pinned();
}

TextServiceReturn TextService::ComposeText(int64_t req_id, std::string &&text) {
  std::vector<std::string> mention_usernames;
  std::smatch m;
  std::regex e("@[a-zA-Z0-9-_]+");
  auto s = text;
  while (std::regex_search(s, m, e)) {
    auto user_mention = m.str();
    user_mention = user_mention.substr(1, user_mention.length());
    mention_usernames.emplace_back(user_mention);
    s = m.suffix().str();
  }

  std::vector<std::string> urls;
  e = "(http://|https://)([a-zA-Z0-9_!~*'().&=+$%-]+)";
  s = text;
  while (std::regex_search(s, m, e)) {
    auto url = m.str();
    urls.emplace_back(url);
    s = m.suffix().str();
  }

  auto target_urls_future = _url_shorten_service_obj.run_async(
      &UrlShortenService::ComposeUrls, req_id, urls);
  auto user_mentions_future = _user_mention_service_obj.run_async(
      &UserMentionService::ComposeUserMentions, req_id,
      std::move(mention_usernames));

  auto target_urls = target_urls_future.get();
  auto user_mentions = user_mentions_future.get();

  std::string updated_text;
  if (!urls.empty()) {
    s = text;
    int idx = 0;
    while (std::regex_search(s, m, e)) {
      auto url = m.str();
      urls.emplace_back(url);
      updated_text += m.prefix().str() + target_urls[idx].shortened_url;
      s = m.suffix().str();
      idx++;
    }
    updated_text += s;
  } else {
    updated_text = text;
  }

  TextServiceReturn text_service_return;
  text_service_return.user_mentions = user_mentions;
  text_service_return.text = updated_text;
  text_service_return.urls = target_urls;
  return text_service_return;
}

}  // namespace social_network

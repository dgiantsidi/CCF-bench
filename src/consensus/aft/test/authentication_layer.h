
#include <cstring>
#include <fmt/printf.h>
#include <iostream>
#include <memory>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <tuple>

namespace authentication
{
#ifdef AUTHENTICATOR
  void print_errors()
  {
    ERR_print_errors_fp(stderr);
    abort();
  }

  void init()
  {
    // Initialise OpenSSL library
    OpenSSL_add_all_algorithms();
    ERR_load_crypto_strings();
  }

  size_t get_hash_len()
  {
    return 32;
  }
  bool is_enabled()
  {
    return true;
  }

  std::tuple<std::unique_ptr<uint8_t[]>, size_t> get_hash(
    const uint8_t* msg, const size_t msg_size)
  {
    // Create a context for hashing
    EVP_MD_CTX* mdctx = EVP_MD_CTX_new();
    if (mdctx == NULL)
      print_errors();

    // Initialise the context for SHA-256
    if (1 != EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL))
      print_errors();

    // Provide the message to be hashed
    if (1 != EVP_DigestUpdate(mdctx, msg, msg_size))
      print_errors();

    // Finalise the hashing process and retrieve the hash
    std::unique_ptr<uint8_t[]> hash_data =
      std::make_unique<uint8_t[]>(EVP_MAX_MD_SIZE);
    unsigned int hash_len;
    if (1 != EVP_DigestFinal_ex(mdctx, hash_data.get(), &hash_len))
      print_errors();
#  if 0
    if (hash_len != EVP_MAX_MD_SIZE)
      fmt::print(
        "{} error happened in computing the hash and hash_len={} != "
        "EVP_MAX_MD_SIZE={}\n",
        __func__,
        hash_len,
        EVP_MAX_MD_SIZE);
#  endif
    // Clean up
    EVP_MD_CTX_free(mdctx);
    EVP_cleanup();
    ERR_free_strings();

    return {std::move(hash_data), hash_len};
  }

  bool verify_hash(
    const uint8_t* message, const size_t msg_sz, const uint8_t* received_hash)
  {
    auto [computed_hash, computed_hash_len] = get_hash(message, msg_sz);

    // Compare the computed hash with the received hash
    return (memcmp(computed_hash.get(), received_hash, computed_hash_len) == 0);
  }
#else
  void print_errors() {}

  void init()
  {
    fmt::print("{} -> no authentication layer used\n", __func__);
  }

  size_t get_hash_len()
  {
    return 0;
  }
  bool is_enabled()
  {
    return false;
  }

  std::tuple<std::unique_ptr<uint8_t[]>, size_t> get_hash(
    const uint8_t* msg, const size_t msg_size)
  {
    return {std::make_unique<uint8_t[]>(0), 0};
  }

  bool verify_hash(
    const uint8_t* message, const size_t msg_sz, const uint8_t* received_hash)
  {
    return true;
  }
#endif
}; // end namespace
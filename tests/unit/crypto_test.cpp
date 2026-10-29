// crypto/: CRC32, SHA-256, Ed25519 keys + sign/verify, key files.
#include "crypto/hash.h"

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <stdexcept>

#include "common/hex.h"
#include "crypto/keys.h"
#include "crypto/signer.h"

using ledger::from_hex;
using ledger::to_hex;
using ledger::crypto::crc32;
using namespace ledger::crypto;

TEST(Crc32, StandardCheckValue) {
  // The published check value for CRC-32/ISO-HDLC (zlib, PNG, Ethernet).
  EXPECT_EQ(crc32("123456789"), 0xCBF43926u);
  EXPECT_EQ(crc32(""), 0u);
}

TEST(Crc32, IncrementalEqualsOneShot) {
  std::string data = "the quick brown fox jumps over the lazy dog";
  uint32_t part = crc32(data.substr(0, 10));
  EXPECT_EQ(crc32(data.substr(10), part), crc32(data));
}

TEST(Crc32, DetectsSingleBitFlip) {
  std::string data(100, 'x');
  uint32_t good = crc32(data);
  for (size_t i = 0; i < data.size(); ++i) {
    std::string bad = data;
    bad[i] ^= 0x01;
    EXPECT_NE(crc32(bad), good) << "flip at byte " << i;
  }
}

TEST(Sha256, StandardVectors) {
  // FIPS 180-2 examples.
  EXPECT_EQ(to_hex(sha256("abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  EXPECT_EQ(to_hex(sha256("")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
}

TEST(Ed25519, Rfc8032TestVector1) {
  // RFC 8032 section 7.1, TEST 1 (empty message). Proves we call libsodium
  // correctly, not just consistently with ourselves.
  KeyPair kp = keypair_from_seed(
      *from_hex("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"));
  EXPECT_EQ(to_hex(kp.public_key),
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a");
  EXPECT_EQ(to_hex(sign("", kp)),
            "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e06522490155"
            "5fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b");
}

TEST(Ed25519, SignVerify) {
  KeyPair alice = generate_keypair();
  KeyPair mallory = generate_keypair();
  std::string msg = "pay bob 30";
  std::string sig = sign(msg, alice);
  EXPECT_EQ(sig.size(), kSignatureSize);

  EXPECT_TRUE(verify(msg, sig, alice.public_key));
  EXPECT_FALSE(verify("pay bob 31", sig, alice.public_key));  // tampered message
  EXPECT_FALSE(verify(msg, sig, mallory.public_key));         // wrong key
  std::string bad = sig;
  bad[10] ^= 1;
  EXPECT_FALSE(verify(msg, bad, alice.public_key));           // tampered signature
  EXPECT_FALSE(verify(msg, sig.substr(0, 63), alice.public_key));  // wrong sizes
  EXPECT_FALSE(verify(msg, sig, ""));
}

TEST(Ed25519, DeterministicSignatures) {
  KeyPair kp = generate_keypair();
  EXPECT_EQ(sign("m", kp), sign("m", kp));
  EXPECT_EQ(keypair_from_seed(kp.seed()).public_key, kp.public_key);
}

TEST(KeyFile, SaveLoadAndPermissions) {
  std::string path = ::testing::TempDir() + "keyfile_test_" + std::to_string(::getpid()) + ".key";
  std::remove(path.c_str());
  KeyPair kp = generate_keypair();
  save_keyfile(path, "alice", kp);

  struct stat st{};
  ASSERT_EQ(::stat(path.c_str(), &st), 0);
  EXPECT_EQ(st.st_mode & 0777, 0600u) << "secret key file must be owner-only";

  KeyPair loaded = load_keyfile(path);
  EXPECT_EQ(loaded.public_key, kp.public_key);
  EXPECT_EQ(loaded.secret_key, kp.secret_key);
  EXPECT_THROW(save_keyfile(path, "alice", generate_keypair()), std::runtime_error);  // no overwrite
  std::remove(path.c_str());
}

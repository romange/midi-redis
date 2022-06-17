// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "string_set/string_set.h"

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <unordered_set>

#include "base/gtest.h"
#include "base/logging.h"


namespace dfly {

using namespace std;

class StringSetTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
  }

  static void TearDownTestSuite() {
  }

  StringSet ss_;
};

TEST_F(StringSetTest, Basic) {
  EXPECT_TRUE(ss_.Add("foo"));
  EXPECT_TRUE(ss_.Add("bar"));
  EXPECT_FALSE(ss_.Add("foo"));
  EXPECT_FALSE(ss_.Add("bar"));
  EXPECT_EQ(2, ss_.size());
}

TEST_F(StringSetTest, Ex1) {
  EXPECT_TRUE(ss_.Add("AA@@@@@@@@@@@@@@"));
  EXPECT_TRUE(ss_.Add("AAA@@@@@@@@@@@@@"));
  EXPECT_TRUE(ss_.Add("AAAAAAAAA@@@@@@@"));
  EXPECT_TRUE(ss_.Add("AAAAAAAAAA@@@@@@"));
  EXPECT_TRUE(ss_.Add("AAAAAAAAAAAAAAA@"));
  EXPECT_TRUE(ss_.Add("BBBBBAAAAAAAAAAA"));
  EXPECT_TRUE(ss_.Add("BBBBBBBBAAAAAAAA"));
  EXPECT_TRUE(ss_.Add("CCCCCBBBBBBBBBBB"));
}

TEST_F(StringSetTest, Many) {
  double max_chain_factor = 0;
  for (unsigned i = 0; i < 8192; ++i) {
    EXPECT_TRUE(ss_.Add(absl::StrCat("xxxxxxxxxxxxxxxxx", i)));
    size_t sz = ss_.size();
    bool should_print = (sz == ss_.bucket_count()) || (sz == ss_.bucket_count() * 0.75);
    if (should_print) {
      double chain_usage = double(ss_.num_chain_entries()) / ss_.size();
      unsigned num_empty = ss_.bucket_count() - ss_.num_used_buckets();
      double empty_factor = double(num_empty) / ss_.bucket_count();

      LOG(INFO) << "chains: " << 100 * chain_usage << ", empty: " << 100 * empty_factor << "% at "
                << ss_.size();
#if 0
      if (ss_.size() == 15) {
        for (unsigned i = 0; i < ss_.bucket_count(); ++i) {
          LOG(INFO) << "[" << i << "]: " << ss_.BucketDepth(i);
        }
        /*ss_.IterateOverBucket(93, [this](const CompactObj& co) {
          LOG(INFO) << "93->" << (co.HashCode() % ss_.bucket_count());
        });*/
      }
#endif
    }
  }
  EXPECT_EQ(8192, ss_.size());

  LOG(INFO) << "max chain factor: " << 100 * max_chain_factor << "%";
  /*size_t iter_len = 0;
  for (auto it = ss_.begin(); it != ss_.end(); ++it) {
    ++iter_len;
  }
  EXPECT_EQ(iter_len, 512);*/
}

#if 0
TEST_F(StringSetTest, IterScan) {
  unordered_set<string> actual, expected;
  auto insert_actual = [&](const CompactObj& val) {
    string tmp;
    val.GetString(&tmp);
    actual.insert(tmp);
  };

  EXPECT_EQ(0, ss_.Scan(0, insert_actual));
  EXPECT_TRUE(actual.empty());

  for (unsigned i = 0; i < 512; ++i) {
    string s = absl::StrCat("x", i);
    expected.insert(s);
    EXPECT_TRUE(ss_.Add(s));
  }


  for (CompactObj& val : ss_) {
    insert_actual(val);
  }

  EXPECT_EQ(actual, expected);
  actual.clear();
  uint32_t cursor = 0;
  do {
    cursor = ss_.Scan(cursor, insert_actual);
  } while (cursor);
  EXPECT_EQ(actual, expected);
}

#endif

}  // namespace dfly
#include <stdio.h>
#include <gtest/gtest.h>

#include "itest_bitmap.cc"

TEST(TestSuite, testCase1) {
        EXPECT_TRUE(1);
}

TEST(TestSuite, testCase2) {
        EXPECT_EQ(1, 1);
        EXPECT_TRUE(1);
}

TEST(TestSuite, testCase3) {
        EXPECT_FALSE(0);
}

int main(int argc, char **argv) {
        testing::InitGoogleTest(&argc, argv);
        return RUN_ALL_TESTS();
}

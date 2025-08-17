#include <gtest/gtest.h>
#include <string>
#include "raft_server.h"

namespace {
    bool matches_either_ip_version(const std::string& result, 
                                 const std::string& ipv4_version,
                                 const std::string& ipv6_version) {
        return result == ipv4_version || result == ipv6_version;
    }

    bool is_ipv4(const std::string& str) {
        struct sockaddr_in sa;
        return inet_pton(AF_INET, str.c_str(), &(sa.sin_addr)) != 0;
    }

    bool is_ipv6_with_brackets(const std::string& str) {
        if (str.length() < 2 || str[0] != '[' || str[str.length() - 1] != ']') {
            return false;
        }

        std::string ipv6 = str.substr(1, str.length() - 2); // Remove [ and ]
        struct sockaddr_in6 sa;
        return inet_pton(AF_INET6, ipv6.c_str(), &(sa.sin6_addr)) != 0;
    }
}

TEST(RaftServerTest, ResolveNodesConfigWithHostNames) {
    ASSERT_EQ("127.0.0.1:8107:8108,127.0.0.1:7107:7108,127.0.0.1:6107:6108",
              ReplicationState::resolve_node_hosts("127.0.0.1:8107:8108,127.0.0.1:7107:7108,127.0.0.1:6107:6108"));

    // Test localhost resolution - should accept either IPv4 or IPv6
    std::string localhost_result1 = ReplicationState::resolve_node_hosts("localhost:8107:8108,localhost:7107:7108,localhost:6107:6108");
    ASSERT_TRUE(matches_either_ip_version(
        localhost_result1,
        "127.0.0.1:8107:8108,127.0.0.1:7107:7108,127.0.0.1:6107:6108",
        "[::1]:8107:8108,[::1]:7107:7108,[::1]:6107:6108"
    )) << "Result was: " << localhost_result1;

    std::string localhost_result2 = ReplicationState::resolve_node_hosts("localhost:8107:8108localhost:7107:7108,localhost:6107:6108");
    ASSERT_TRUE(matches_either_ip_version(
        localhost_result2,
        "localhost:8107:8108localhost:7107:7108,127.0.0.1:6107:6108",
        "localhost:8107:8108localhost:7107:7108,[::1]:6107:6108"
    )) << "Result was: " << localhost_result2;

    // hostname must be less than 64 chars
    ASSERT_EQ("",
              ReplicationState::resolve_node_hosts("typesense-node-2.typesense-service.typesense-"
                                                   "namespace.svc.cluster.local:6107:6108"));
}

TEST(RaftServerTest, ResolveNodesConfigWithIPv6) {
    // Basic IPv6 addresses
    ASSERT_EQ("[2001:db8::1]:8107:8108,[2001:db8::2]:7107:7108",
              ReplicationState::resolve_node_hosts("[2001:db8::1]:8107:8108,[2001:db8::2]:7107:7108"));

    // IPv6 with IPv4 mixed
    ASSERT_EQ("[2001:db8::1]:8107:8108,127.0.0.1:7107:7108",
              ReplicationState::resolve_node_hosts("[2001:db8::1]:8107:8108,127.0.0.1:7107:7108"));

    // IPv6 localhost
    ASSERT_EQ("[::1]:8107:8108",
              ReplicationState::resolve_node_hosts("[::1]:8107:8108"));

    // Malformed IPv6 inputs should be passed through unchanged
    ASSERT_EQ("[2001:db8::1:8107:8108",  // Missing closing bracket
              ReplicationState::resolve_node_hosts("[2001:db8::1:8107:8108"));

    // IPv6 with zone index
    ASSERT_EQ("[fe80::1%eth0]:8107:8108",
              ReplicationState::resolve_node_hosts("[fe80::1%eth0]:8107:8108"));

    // Test with real IPv6 hostname resolution - need to skip if resolution fails
    std::string ipv6_result = ReplicationState::resolve_node_hosts("ipv6.test-ipv6.com:8107:8108");
    if (!ipv6_result.empty()) {
        EXPECT_TRUE(ipv6_result.find('[') == 0);  // Should start with '[' for IPv6
        EXPECT_TRUE(ipv6_result.find("]:8107:8108") != std::string::npos);
    }
}

TEST(Hostname2IPStrTest, IPAddresses) {
    // Test IPv4 addresses - should return unchanged
    ASSERT_EQ("127.0.0.1", ReplicationState::hostname2ipstr("127.0.0.1"));
    ASSERT_EQ("192.168.1.1", ReplicationState::hostname2ipstr("192.168.1.1"));

    // Test IPv6 addresses - should return unchanged if already in brackets
    ASSERT_EQ("[::1]", ReplicationState::hostname2ipstr("[::1]"));
    ASSERT_EQ("[2001:db8::1]", ReplicationState::hostname2ipstr("[2001:db8::1]"));
}

TEST(Hostname2IPStrTest, Localhost) {
    std::string result = ReplicationState::hostname2ipstr("localhost");

    // Should resolve to either 127.0.0.1 or [::1]
    ASSERT_TRUE(result == "127.0.0.1" || result == "[::1]")
        << "localhost resolved to: " << result;
}

TEST(Hostname2IPStrTest, InvalidHostnames) {
    // Test hostname that's too long (>64 chars)
    std::string long_hostname(65, 'a');
    ASSERT_EQ("", ReplicationState::hostname2ipstr(long_hostname));

    // Test non-existent hostname - implementation returns original hostname
    ASSERT_EQ("non.existent.hostname.local",
              ReplicationState::hostname2ipstr("non.existent.hostname.local"));
}

TEST(Hostname2IPStrTest, PublicHostnames) {
    // Test IPv6-only hostname resolution
    std::string ipv6_result = ReplicationState::hostname2ipstr("ipv6.test-ipv6.com");
    if (!ipv6_result.empty() && ipv6_result != "ipv6.test-ipv6.com") {
        EXPECT_TRUE(is_ipv6_with_brackets(ipv6_result))
            << "ipv6.test-ipv6.com did not resolve to IPv6: " << ipv6_result;
    }

    // Test IPv4-only hostname resolution
    std::string ipv4_result = ReplicationState::hostname2ipstr("ipv4.test-ipv6.com");
    if (!ipv4_result.empty() && ipv4_result != "ipv4.test-ipv6.com") {
        EXPECT_TRUE(is_ipv4(ipv4_result))
            << "ipv4.test-ipv6.com did not resolve to IPv4: " << ipv4_result;
    }
}

// Test the new parse_node_configuration method (replaces resolve_node_hosts)
TEST(RaftServerTest, ParseNodeConfiguration) {
    // Test mixed hostname and IP configuration
    std::string mixed_config = "node1.example.com:8107:8108,192.168.1.10:8107:8108,node2.internal:8107:8108";
    NodeConfiguration config = ReplicationState::parse_node_configuration(mixed_config);
    
    EXPECT_EQ(2, config.hostname_nodes.size());
    EXPECT_EQ(1, config.ip_nodes.size());
    EXPECT_TRUE(config.has_hostnames());
    EXPECT_TRUE(config.has_ips());
    EXPECT_FALSE(config.empty());
    EXPECT_EQ(3, config.total_nodes());
}

TEST(RaftServerTest, ParseNodeConfigurationIPv6) {
    // Test IPv6 configuration
    std::string ipv6_config = "[2001:db8::1]:8107:8108,[2001:db8::2]:8107:8108";
    NodeConfiguration config = ReplicationState::parse_node_configuration(ipv6_config);
    
    EXPECT_EQ(0, config.hostname_nodes.size());
    EXPECT_EQ(2, config.ip_nodes.size());
    EXPECT_FALSE(config.has_hostnames());
    EXPECT_TRUE(config.has_ips());
}

TEST(RaftServerTest, ExtractHostnameFromNode) {
    // Test hostname extraction
    EXPECT_EQ("node1.example.com", 
              ReplicationState::extract_hostname_from_node("node1.example.com:8107:8108"));
    EXPECT_EQ("internal.service", 
              ReplicationState::extract_hostname_from_node("internal.service:9000:9001"));
    
    // Test IP addresses (should return empty)
    EXPECT_EQ("", ReplicationState::extract_hostname_from_node("192.168.1.10:8107:8108"));
    EXPECT_EQ("", ReplicationState::extract_hostname_from_node("[2001:db8::1]:8107:8108"));
    
    // Test malformed strings
    EXPECT_EQ("", ReplicationState::extract_hostname_from_node("malformed"));
    EXPECT_EQ("", ReplicationState::extract_hostname_from_node("too:many:colons:8107:8108"));
}
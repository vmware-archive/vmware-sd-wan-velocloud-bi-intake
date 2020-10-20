-- phpMyAdmin SQL Dump
-- version 4.6.6deb5
-- https://www.phpmyadmin.net/
--
-- Host: localhost:3306
-- Generation Time: Aug 13, 2020 at 07:51 PM
-- Server version: 5.7.29-32
-- PHP Version: 7.3.18-1+ubuntu18.04.1+deb.sury.org+1

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `customer`
--

CREATE DATABASE IF NOT EXISTS Customer;

--
-- Select Database: `customer`
--

USE DATABASE Customer;


-- --------------------------------------------------------

--
-- Table structure for table `Customer`
--

CREATE TABLE `Customer` (
  `Customer_ID_VCO` varchar(60) NOT NULL,
  `VCO` varchar(500) CHARACTER SET utf8mb4 NOT NULL,
  `Customer_NAME` varchar(255) NOT NULL,
  `customer_status` varchar(254) DEFAULT NULL,
  `enterprise_id` int(11) DEFAULT NULL,
  `Version` varchar(60) NOT NULL DEFAULT '',
  `Segments_bool` tinyint(1) NOT NULL DEFAULT '0',
  `Segments_num` int(11) NOT NULL DEFAULT '0',
  `NVS_bool` tinyint(1) NOT NULL DEFAULT '0',
  `NVS_num` int(11) NOT NULL DEFAULT '0',
  `CVH_bool` tinyint(1) NOT NULL DEFAULT '0',
  `CVH_num` int(11) NOT NULL DEFAULT '0',
  `VNF_bool` tinyint(1) NOT NULL DEFAULT '0',
  `HA_bool` tinyint(1) NOT NULL DEFAULT '0',
  `Cluster_bool` tinyint(1) NOT NULL DEFAULT '0',
  `VRRP_bool` tinyint(1) NOT NULL DEFAULT '0',
  `OSPF_BOOL` tinyint(4) NOT NULL DEFAULT '0',
  `BGP_BOOL` tinyint(4) NOT NULL DEFAULT '0',
  `ROUTE_NUM` bigint(20) NOT NULL DEFAULT '0',
  `ROUTE_CHANGE` int(11) NOT NULL DEFAULT '0',
  `MPLS_BOOL` tinyint(4) NOT NULL DEFAULT '0',
  `WIRELESS_LINK` tinyint(4) NOT NULL DEFAULT '0',
  `BACKUP_LINK` tinyint(4) NOT NULL DEFAULT '0',
  `Partner` char(60) NOT NULL DEFAULT '',
  `Partner_Email` varchar(2000) NOT NULL DEFAULT '',
  `lastUpdated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `CustomerCreationDate` datetime DEFAULT CURRENT_TIMESTAMP,
  `customer_marketing_name` varchar(255) DEFAULT NULL,
  `customer_domain` varchar(60) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `CustomerAttributes`
--

CREATE TABLE `CustomerAttributes` (
  `customer_uuid` varchar(60) NOT NULL,
  `name` varchar(60) NOT NULL,
  `filter_val` varchar(100) DEFAULT NULL,
  `used` tinyint(1) DEFAULT NULL,
  `num` int(11) DEFAULT NULL,
  `text` varchar(2000) DEFAULT NULL,
  `last_updated` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `DailyQOE`
--

CREATE TABLE `DailyQOE` (
  `Date` datetime NOT NULL,
  `EdgeID` varchar(255) NOT NULL,
  `LinkUUID` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `Score` float DEFAULT NULL,
  `lowest_linkscore` float DEFAULT NULL,
  `LinkBlackouts` int(4) DEFAULT NULL,
  `LinkBlackoutDuration` float DEFAULT NULL,
  `LinkBrownouts` int(4) DEFAULT NULL,
  `LinkBrownoutDuration` float DEFAULT NULL,
  `LinkID` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `Edge`
--

CREATE TABLE `Edge` (
  `EdgeID` varchar(60) CHARACTER SET utf8mb4 NOT NULL,
  `EdgeName` varchar(60) DEFAULT NULL,
  `Customer_ID_VCO` varchar(60) NOT NULL,
  `Profile_ID` text,
  `lastUpdated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `License` text,
  `Bandwidth` int(11) NOT NULL DEFAULT '0',
  `Activation_Status` text,
  `Edge_Status` text,
  `Model` varchar(60) DEFAULT NULL,
  `Activated_Day` datetime DEFAULT CURRENT_TIMESTAMP,
  `Activated_Days` int(11) DEFAULT '0',
  `HA` text,
  `Private_LINKS_num` int(11) NOT NULL DEFAULT '0',
  `Private_LINKS_bool` tinyint(1) NOT NULL DEFAULT '0',
  `Private_LINKS_vlan` int(11) NOT NULL DEFAULT '0',
  `Public_LINKS_num` int(11) NOT NULL DEFAULT '0',
  `Public_LINKS_bol` tinyint(1) NOT NULL DEFAULT '0',
  `Public_LINKS_vlan` int(11) NOT NULL DEFAULT '0',
  `Public_LINKS_BACKUP` int(11) NOT NULL DEFAULT '0',
  `bgp_bool` tinyint(1) NOT NULL DEFAULT '0',
  `ospf_bool` tinyint(1) NOT NULL DEFAULT '0',
  `netflow_bool` tinyint(1) NOT NULL DEFAULT '0',
  `static_routes_bool` tinyint(1) NOT NULL DEFAULT '0',
  `static_routes_num` int(1) NOT NULL DEFAULT '0',
  `Multicast_bool` tinyint(1) NOT NULL DEFAULT '0',
  `Firewall_rules_bool` tinyint(1) NOT NULL DEFAULT '0',
  `Firewall_rules_num` int(11) NOT NULL DEFAULT '0',
  `Firewall_rules_in_bool` tinyint(1) NOT NULL DEFAULT '0',
  `Firewall_rules_out_bool` tinyint(1) NOT NULL DEFAULT '0',
  `Business_policy_bool` tinyint(1) NOT NULL DEFAULT '0',
  `Business_policy_num` int(11) NOT NULL DEFAULT '0',
  `PUBLIC_LINKS_WIRELESS` int(11) NOT NULL DEFAULT '0',
  `Firewall_Edge_Specific` tinyint(1) NOT NULL DEFAULT '0',
  `WAN_Edge_Specific` tinyint(1) NOT NULL DEFAULT '0',
  `QOS_Edge_Specific` tinyint(1) NOT NULL DEFAULT '0',
  `Device_Settings_Edge_Specific` tinyint(1) NOT NULL DEFAULT '0',
  `Score` float NOT NULL DEFAULT '0',
  `UPLINK_USAGE` int(11) NOT NULL DEFAULT '0',
  `DOWNLINK_USAGE` int(11) NOT NULL DEFAULT '0',
  `Version` varchar(60) DEFAULT NULL,
  `HUB` tinyint(1) NOT NULL DEFAULT '0',
  `ENGINEER` varchar(50) DEFAULT NULL,
  `SerialNumber` varchar(64) DEFAULT 'Not set',
  `ha_serial` varchar(60) DEFAULT NULL,
  `Certificate` varchar(64) DEFAULT 'Not set',
  `lat` float DEFAULT NULL,
  `lon` float DEFAULT NULL,
  `Geospecific` varchar(255) DEFAULT NULL,
  `Country` varchar(64) DEFAULT 'Not set',
  `State` varchar(64) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT 'Not set',
  `PostalCode` varchar(64) DEFAULT 'Not set',
  `City` varchar(64) CHARACTER SET utf8 COLLATE utf8_unicode_520_ci DEFAULT 'Not Set',
  `street_address` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  `geopy_loc` tinyint(1) DEFAULT NULL,
  `stateful_firewall` tinyint(1) NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `EdgeAttributes`
--

CREATE TABLE `EdgeAttributes` (
  `edge_uuid` varchar(60) CHARACTER SET utf8mb4 NOT NULL,
  `name` varchar(60) NOT NULL,
  `filter_val` varchar(100) DEFAULT NULL,
  `used` tinyint(1) DEFAULT NULL,
  `num` int(11) DEFAULT NULL,
  `text` varchar(2000) DEFAULT NULL,
  `last_updated` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `Events`
--

CREATE TABLE `Events` (
  `Date` datetime NOT NULL,
  `EdgeID` varchar(60) NOT NULL,
  `Name` varchar(60) NOT NULL,
  `Type` varchar(60) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `License`
--

CREATE TABLE `License` (
  `EdgeID` varchar(60) NOT NULL,
  `sku` varchar(120) DEFAULT NULL,
  `start` datetime DEFAULT NULL,
  `end` datetime DEFAULT NULL,
  `active` tinyint(1) NOT NULL DEFAULT '0',
  `termMonths` int(11) DEFAULT NULL,
  `edition` varchar(30) NOT NULL,
  `bandwidthTier` varchar(8) NOT NULL,
  `addOns` varchar(60) NOT NULL,
  `licenseGap` tinyint(1) NOT NULL DEFAULT '0',
  `bandwidthTierObserved` varchar(8) DEFAULT NULL,
  `upsellOportunity` bigint(20) NOT NULL DEFAULT '0',
  `highest_throughput_in_mbps` varchar(8) DEFAULT NULL,
  `feature_set` varchar(60) DEFAULT NULL,
  `b2b_via_gw` tinyint(1) NOT NULL DEFAULT '0',
  `pb_via_gw` tinyint(1) NOT NULL DEFAULT '0',
  `css_via_gw` tinyint(1) NOT NULL DEFAULT '0',
  `nvs_via_gw` tinyint(1) NOT NULL DEFAULT '0',
  `pb_internet_via_hub` tinyint(1) NOT NULL DEFAULT '0',
  `pb_internet_via_direct` tinyint(1) NOT NULL DEFAULT '0',
  `b2b_via_hub` tinyint(1) NOT NULL DEFAULT '0',
  `fifth_top_throughput` float DEFAULT NULL,
  `tenth_top_throughput` float DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `LinkQuality`
--

CREATE TABLE `LinkQuality` (
  `Date` datetime NOT NULL,
  `EdgeID` varchar(255) DEFAULT NULL,
  `LinkUUID` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `Score` float DEFAULT NULL,
  `YellowBrownout` int(60) DEFAULT NULL,
  `RedBrownout` int(60) DEFAULT NULL,
  `Unknown` int(60) DEFAULT NULL,
  `Blackout` int(60) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `Links`
--

CREATE TABLE `Links` (
  `EdgeID` varchar(255) NOT NULL,
  `LinkUUID` varchar(255) NOT NULL,
  `LinkName` varchar(255) NOT NULL,
  `ISP` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  `Interface` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  `Latitude` float NOT NULL,
  `Longitude` float NOT NULL,
  `NetworkSide` varchar(255) NOT NULL,
  `Networktype` varchar(255) NOT NULL,
  `LinkIPAddress` varchar(255) NOT NULL,
  `MTU` int(11) NOT NULL,
  `OverlayType` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `Linktype` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `LinkMode` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `VLANID` int(11) NOT NULL,
  `LinkID` varchar(255) NOT NULL,
  `lastUpdated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `VCO`
--

CREATE TABLE `VCO` (
  `name` varchar(255) CHARACTER SET utf8mb4 NOT NULL,
  `link` varchar(500) CHARACTER SET utf8mb4 NOT NULL,
  `partner` varchar(255) CHARACTER SET utf8mb4 DEFAULT NULL,
  `version` varchar(20) CHARACTER SET utf8mb4 DEFAULT NULL,
  `last_updated` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `VCOAttributes`
--

CREATE TABLE `VCOAttributes` (
  `vco_link` varchar(255) CHARACTER SET utf8mb4 NOT NULL,
  `name` varchar(60) CHARACTER SET utf8mb4 NOT NULL,
  `filter_val` varchar(100) DEFAULT NULL,
  `used` tinyint(1) DEFAULT NULL,
  `num` int(11) DEFAULT NULL,
  `text` varchar(500) CHARACTER SET utf8mb4 DEFAULT NULL,
  `last_updated` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------



-- --------------------------------------------------------



-- --------------------------------------------------------


-- Indexes for dumped tables
--

--
-- Indexes for table `Customer`
--
ALTER TABLE `Customer`
  ADD PRIMARY KEY (`Customer_ID_VCO`),
  ADD UNIQUE KEY `Customer_ID_VCO` (`Customer_ID_VCO`);

--
-- Indexes for table `CustomerAttributes`
--
ALTER TABLE `CustomerAttributes`
  ADD PRIMARY KEY (`customer_uuid`,`name`);

--
-- Indexes for table `DailyQOE`
--
ALTER TABLE `DailyQOE`
  ADD UNIQUE KEY `Date` (`Date`,`LinkUUID`);

--
-- Indexes for table `Edge`
--
ALTER TABLE `Edge`
  ADD PRIMARY KEY (`EdgeID`),
  ADD UNIQUE KEY `EdgeID` (`EdgeID`),
  ADD KEY `Customer_ID_VCO` (`Customer_ID_VCO`);

--
-- Indexes for table `EdgeAttributes`
--
ALTER TABLE `EdgeAttributes`
  ADD PRIMARY KEY (`edge_uuid`,`name`);

--
-- Indexes for table `Events`
--
ALTER TABLE `Events`
  ADD UNIQUE KEY `UNIQUE_ID` (`Date`,`EdgeID`,`Name`);

--
-- Indexes for table `License`
--
ALTER TABLE `License`
  ADD PRIMARY KEY (`EdgeID`);

--
-- Indexes for table `LinkQuality`
--
ALTER TABLE `LinkQuality`
  ADD UNIQUE KEY `unique_index` (`Date`,`LinkUUID`);

--
-- Indexes for table `Links`
--
ALTER TABLE `Links`
  ADD PRIMARY KEY (`LinkUUID`),
  ADD UNIQUE KEY `LinkUUID` (`LinkUUID`);

--
-- Indexes for table `VCO`
--
ALTER TABLE `VCO`
  ADD PRIMARY KEY (`link`),
  ADD UNIQUE KEY `link` (`link`),
  ADD UNIQUE KEY `name` (`name`);

--
-- Indexes for table `VCOAttributes`
--
ALTER TABLE `VCOAttributes`
  ADD PRIMARY KEY (`vco_link`,`name`);





--
-- Constraints for table `CustomerAttributes`
--
ALTER TABLE `CustomerAttributes`
  ADD CONSTRAINT `Customer_ibfk_1` FOREIGN KEY (`customer_uuid`) REFERENCES `Customer` (`Customer_ID_VCO`);

--
-- Constraints for table `Edge`
--
ALTER TABLE `Edge`
  ADD CONSTRAINT `Customer_ibfk_2` FOREIGN KEY (`Customer_ID_VCO`) REFERENCES `Customer` (`Customer_ID_VCO`);

--
-- Constraints for table `EdgeAttributes`
--
ALTER TABLE `EdgeAttributes`
  ADD CONSTRAINT `Edge_ibfk_2` FOREIGN KEY (`edge_uuid`) REFERENCES `Edge` (`EdgeID`);

--
--
-- Constraints for table `VCOAttributes`
--
ALTER TABLE `VCOAttributes`
  ADD CONSTRAINT `VCO_ibfk_1` FOREIGN KEY (`vco_link`) REFERENCES `VCO` (`link`);

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;

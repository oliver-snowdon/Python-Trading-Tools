-- phpMyAdmin SQL Dump
-- version 4.6.6deb5
-- https://www.phpmyadmin.net/
--
-- Host: localhost:3306
-- Generation Time: Apr 17, 2019 at 05:18 AM
-- Server version: 5.7.25-0ubuntu0.18.04.2
-- PHP Version: 7.2.15-0ubuntu0.18.04.2

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `trader`
--

-- --------------------------------------------------------

--
-- Table structure for table `pairs`
--

CREATE TABLE `pairs` (
  `id` int(11) NOT NULL,
  `pair` varchar(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `runs`
--

CREATE TABLE `runs` (
  `id` int(11) NOT NULL,
  `node` text NOT NULL,
  `remote_run_id` int(11) NOT NULL DEFAULT '-1',
  `pairs` text NOT NULL,
  `first_timestamp` decimal(16,6) DEFAULT NULL,
  `last_timestamp` decimal(16,6) DEFAULT NULL,
  `error` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `spreads`
--

CREATE TABLE `spreads` (
  `id` int(11) NOT NULL,
  `run_id` int(11) NOT NULL,
  `pair_id` int(11) NOT NULL,
  `ask` decimal(15,5) NOT NULL,
  `bid` decimal(15,5) NOT NULL,
  `timestamp` decimal(16,6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `trades`
--

CREATE TABLE `trades` (
  `id` int(11) NOT NULL,
  `run_id` int(11) NOT NULL,
  `pair_id` int(11) NOT NULL,
  `price` decimal(15,5) NOT NULL,
  `amount` decimal(18,8) NOT NULL,
  `timestamp` decimal(16,6) NOT NULL,
  `buy_or_sell` varchar(1) NOT NULL,
  `market_or_limit` varchar(1) NOT NULL,
  `misc` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `pairs`
--
ALTER TABLE `pairs`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `pair` (`pair`);

--
-- Indexes for table `runs`
--
ALTER TABLE `runs`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `spreads`
--
ALTER TABLE `spreads`
  ADD PRIMARY KEY (`id`),
  ADD KEY `run_id` (`run_id`),
  ADD KEY `pair_id` (`pair_id`),
  ADD KEY `timestamp` (`timestamp`),
  ADD KEY `run_id_timestamp` (`run_id`,`timestamp`),
  ADD KEY `pair_id_timestamp` (`pair_id`,`timestamp`),
  ADD KEY `run_id_pair_id_timestamp` (`run_id`,`pair_id`,`timestamp`);

--
-- Indexes for table `trades`
--
ALTER TABLE `trades`
  ADD PRIMARY KEY (`id`),
  ADD KEY `run_id` (`run_id`),
  ADD KEY `pair_id` (`pair_id`),
  ADD KEY `timestamp` (`timestamp`),
  ADD KEY `run_id_timestamp` (`run_id`,`timestamp`),
  ADD KEY `pair_id_timestamp` (`pair_id`,`timestamp`),
  ADD KEY `run_id_pair_id_timestamp` (`run_id`,`pair_id`,`timestamp`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `pairs`
--
ALTER TABLE `pairs`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `runs`
--
ALTER TABLE `runs`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `spreads`
--
ALTER TABLE `spreads`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `trades`
--
ALTER TABLE `trades`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;

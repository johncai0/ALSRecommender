CREATE TABLE `activity_info_table` (
  `hor` int(11) NOT NULL,
  `activity` varchar(64) NOT NULL,
  `day` varchar(64) NOT NULL,
  `pv` int(11) NOT NULL,
  `avgtime` float NOT NULL,
  `aloneips` int(11) NOT NULL,
  `aloneusers` int(11) NOT NULL,
  `onlineusers` int(11) NOT NULL,
  `adshow` int(11) NOT NULL,
  `adclick` int(11) NOT NULL,
  `resourcenum` int(11) NOT NULL,
  PRIMARY KEY (`day`,`activity`,`hor`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `all_info_table` (
  `hor` int(11) NOT NULL,
  `day` varchar(64) NOT NULL,
  `pv` int(11) NOT NULL,
  `avgtime` float NOT NULL,
  `aloneips` int(11) NOT NULL,
  `aloneusers` int(11) NOT NULL,
  `onlineusers` int(11) NOT NULL,
  `adshow` int(11) NOT NULL,
  `adclick` int(11) NOT NULL,
  `resourcenum` int(11) NOT NULL,
  PRIMARY KEY (`day`,`hor`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
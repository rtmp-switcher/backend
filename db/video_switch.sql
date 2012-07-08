SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL';

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `video_switch`.`channel_types`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `video_switch`.`channel_types` ;

SHOW WARNINGS;
CREATE  TABLE IF NOT EXISTS `video_switch`.`channel_types` (
  `id` INT NOT NULL AUTO_INCREMENT ,
  `chan_type` VARCHAR(45) NOT NULL ,
  `description` VARCHAR(256) NULL ,
  PRIMARY KEY (`id`) )
ENGINE = InnoDB, 
COMMENT = 'Types of the channels: RTMP IN, RTMP OUT, FLV (HTTP URL)' ;

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `video_switch`.`channels`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `video_switch`.`channels` ;

SHOW WARNINGS;
CREATE  TABLE IF NOT EXISTS `video_switch`.`channels` (
  `id` INT NULL AUTO_INCREMENT ,
  `name` VARCHAR(45) NOT NULL ,
  `is_enabled` TINYINT(1)  NOT NULL DEFAULT 1 ,
  `comment` VARCHAR(256) NULL ,
  `chan_type` INT NOT NULL ,
  `url` TEXT NOT NULL COMMENT 'HTTP URL' ,
  `bkp_folder` VARCHAR(256) NULL COMMENT 'Relative path!' ,
  PRIMARY KEY (`id`, `chan_type`) ,
  INDEX `fk_chan_type` (`chan_type` ASC) ,
  CONSTRAINT `fk_chan_type`
    FOREIGN KEY (`chan_type` )
    REFERENCES `video_switch`.`channel_types` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB, 
COMMENT = 'Channels in the system' ;

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `video_switch`.`channel_details`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `video_switch`.`channel_details` ;

SHOW WARNINGS;
CREATE  TABLE IF NOT EXISTS `video_switch`.`channel_details` (
  `id` INT NOT NULL AUTO_INCREMENT ,
  `tm_created` TIMESTAMP NOT NULL DEFAULT   CURRENT_TIMESTAMP ,
  `channel` INT NOT NULL ,
  `app` VARCHAR(256) NOT NULL COMMENT 'VIDEO_app' ,
  `playPath` VARCHAR(256) NOT NULL COMMENT 'VIDEO_playPath' ,
  `flashVer` VARCHAR(45) NOT NULL COMMENT 'VIDEO_flashVer' ,
  `swfUrl` TEXT NOT NULL COMMENT 'VIDEO_swfUrl' ,
  `url` TEXT NOT NULL COMMENT 'VIDEO_url' ,
  `pageUrl` TEXT NOT NULL COMMENT 'VIDEO_pageURL' ,
  `tcUrl` TEXT NOT NULL COMMENT 'VIDEO_tcURL' ,
  PRIMARY KEY (`id`, `channel`) ,
  INDEX `fk_chan_id` (`channel` ASC) ,
  CONSTRAINT `fk_chan_id`
    FOREIGN KEY (`channel` )
    REFERENCES `video_switch`.`channels` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB, 
COMMENT = 'Detailed information about the specific channel with time.' ;

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `video_switch`.`channel_states`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `video_switch`.`channel_states` ;

SHOW WARNINGS;
CREATE  TABLE IF NOT EXISTS `video_switch`.`channel_states` (
  `id` INT NOT NULL AUTO_INCREMENT ,
  `name` VARCHAR(45) NOT NULL ,
  `description` VARCHAR(256) NULL ,
  PRIMARY KEY (`id`) )
ENGINE = InnoDB, 
COMMENT = 'Possible channel states: live, down. We can put here here mo' /* comment truncated */ ;

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `video_switch`.`channel_status`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `video_switch`.`channel_status` ;

SHOW WARNINGS;
CREATE  TABLE IF NOT EXISTS `video_switch`.`channel_status` (
  `id` INT NOT NULL AUTO_INCREMENT ,
  `tm_updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ,
  `state` INT NOT NULL ,
  `channel` INT NOT NULL ,
  `connected_to` INT NULL DEFAULT NULL COMMENT 'Valid ONLY for incoming channels. They can be connected to outgoing channels' ,
  PRIMARY KEY (`id`, `state`, `channel`) ,
  INDEX `fk_state` (`state` ASC) ,
  INDEX `fk_channel` (`channel` ASC) ,
  INDEX `fk_connected_to` (`connected_to` ASC) ,
  CONSTRAINT `fk_state`
    FOREIGN KEY (`state` )
    REFERENCES `video_switch`.`channel_states` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_channel`
    FOREIGN KEY (`channel` )
    REFERENCES `video_switch`.`channels` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_connected_to`
    FOREIGN KEY (`connected_to` )
    REFERENCES `video_switch`.`channels` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB, 
COMMENT = 'Status of the channel' ;

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `video_switch`.`task_types`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `video_switch`.`task_types` ;

SHOW WARNINGS;
CREATE  TABLE IF NOT EXISTS `video_switch`.`task_types` (
  `id` INT NOT NULL AUTO_INCREMENT ,
  `name` VARCHAR(45) NOT NULL ,
  `description` VARCHAR(256) NULL ,
  PRIMARY KEY (`id`) )
ENGINE = InnoDB, 
COMMENT = 'Task types dictionary' ;

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `video_switch`.`tasks`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `video_switch`.`tasks` ;

SHOW WARNINGS;
CREATE  TABLE IF NOT EXISTS `video_switch`.`tasks` (
  `id` INT NOT NULL AUTO_INCREMENT ,
  `task_type` INT NOT NULL ,
  `tm_created` TIMESTAMP NOT NULL DEFAULT   CURRENT_TIMESTAMP ,
  `arg1` INT NULL ,
  `arg2` INT NULL ,
  `arg_string` VARCHAR(45) NULL ,
  PRIMARY KEY (`id`, `task_type`) ,
  INDEX `fk_tasks_type` (`task_type` ASC) ,
  CONSTRAINT `fk_tasks_type`
    FOREIGN KEY (`task_type` )
    REFERENCES `video_switch`.`task_types` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB, 
COMMENT = 'Tasks received from the clients' ;

SHOW WARNINGS;

CREATE USER `video_switch` IDENTIFIED BY 'Nhe,fleh2?';

grant ALL on TABLE `video_switch`.`channel_states` to video_switch;
grant ALL on TABLE `video_switch`.`channel_details` to video_switch;
grant ALL on TABLE `video_switch`.`channel_types` to video_switch;
grant ALL on TABLE `video_switch`.`channels` to video_switch;
grant ALL on TABLE `video_switch`.`channel_status` to video_switch;
SHOW WARNINGS;

SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

-- -----------------------------------------------------
-- Data for table `video_switch`.`channel_types`
-- -----------------------------------------------------
START TRANSACTION;
USE `video_switch`;
INSERT INTO `video_switch`.`channel_types` (`id`, `chan_type`, `description`) VALUES (NULL, 'RTMP_IN', 'Incoming RTMP');
INSERT INTO `video_switch`.`channel_types` (`id`, `chan_type`, `description`) VALUES (NULL, 'RTMP_OUT', 'Outgoing RTMP (broadcasting channel)');

COMMIT;

-- -----------------------------------------------------
-- Data for table `video_switch`.`channel_states`
-- -----------------------------------------------------
START TRANSACTION;
USE `video_switch`;
INSERT INTO `video_switch`.`channel_states` (`id`, `name`, `description`) VALUES (NULL, 'UP', 'Channel is UP');
INSERT INTO `video_switch`.`channel_states` (`id`, `name`, `description`) VALUES (NULL, 'DOWN', 'Channel is DOWN');

COMMIT;

-- -----------------------------------------------------
-- Data for table `video_switch`.`task_types`
-- -----------------------------------------------------
START TRANSACTION;
USE `video_switch`;
INSERT INTO `video_switch`.`task_types` (`id`, `name`, `description`) VALUES (NULL, 'CONNECT', 'Connect incoming and outgoing channels. Arg1: incoming channel id. Arg2: outhoing channel id.');
INSERT INTO `video_switch`.`task_types` (`id`, `name`, `description`) VALUES (NULL, 'SYNC', 'Re-read the channel_details. New data has been added to the table.');

COMMIT;

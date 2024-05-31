// Sometimes you need to run queries that join tables which produce arbitrary column names.
// This file shows how to handle arbitrary query column names by assigning aliases, then scanning rows into a slice array that contains structs with embedded structs.
// Take note of the relationship between struct tags and query aliasing.
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

var schema = `
CREATE TABLE TEAM
(
    ID         INTEGER PRIMARY KEY,
    NAME VARCHAR(35) UNIQUE,
    CREATED_AT timestamp   NOT NULL DEFAULT now(),
    UPDATED_AT timestamp   NOT NULL DEFAULT now(),
    DELETED_AT timestamp
);

CREATE TABLE GAME
(
    ID             INTEGER PRIMARY KEY,
    HOME_TEAM_ID   INTEGER    NOT NULL,
    AWAY_TEAM_ID   INTEGER    NOT NULL,
    GAME_DATE_TIME timestamp,
    CREATED_AT     timestamp  NOT NULL DEFAULT now(),
    UPDATED_AT     timestamp  NOT NULL DEFAULT now(),
    DELETED_AT     timestamp,
    foreign key (HOME_TEAM_ID) references TEAM (ID),
    foreign key (AWAY_TEAM_ID) references TEAM (ID)
);

CREATE TABLE GAME_OFFENSIVE_STATS
(
    ID                        SERIAL PRIMARY KEY,
    TEAM_ID                   INTEGER   NOT NULL,
    GAME_ID                   INTEGER   NOT NULL,
    OFFENSE_AT_BATS           REAL       NOT NULL DEFAULT 0,
    CREATED_AT                timestamp NOT NULL DEFAULT now(),
    UPDATED_AT                timestamp NOT NULL DEFAULT now(),
    DELETED_AT                timestamp,
    foreign key (TEAM_ID) references TEAM (ID),
    foreign key (GAME_ID) references GAME (ID)
);

CREATE TABLE GAME_DEFENSIVE_STATS
(
    ID                      SERIAL PRIMARY KEY,
    TEAM_ID                 INTEGER   NOT NULL,
    GAME_ID                 INTEGER   NOT NULL,
    DEFENSE_ASSISTS         REAL       NOT NULL DEFAULT 0,
    CREATED_AT              timestamp NOT NULL DEFAULT now(),
    UPDATED_AT              timestamp NOT NULL DEFAULT now(),
    DELETED_AT              timestamp,
    foreign key (TEAM_ID) references TEAM (ID),
    foreign key (GAME_ID) references GAME (ID)
);

CREATE TABLE GAME_PITCHING_STATS
(
    ID                         SERIAL PRIMARY KEY,
    TEAM_ID                    INT       NOT NULL,
    GAME_ID                    INT       NOT NULL,
    PITCHING_AIR_OUTS          REAL       NOT NULL DEFAULT 0,
    CREATED_AT                 timestamp NOT NULL DEFAULT now(),
    UPDATED_AT                 timestamp NOT NULL DEFAULT now(),
    DELETED_AT                 timestamp,
    foreign key (TEAM_ID) references TEAM (ID),
    foreign key (GAME_ID) references GAME (ID)
);`

const getTeamStats = `select g.id as "game.id",
       g.home_team_id as "game.home_team_id",
       g.away_team_id as "game.away_team_id",
       g.game_date_time as "game.game_date_time",
       g.created_at as "game.created_at",
       g.updated_at as "game.updated_at",
       g.deleted_at as "game.deleted_at",
       gds.id as "defense.id",
       gds.team_id as "defense.team_id",
       gds.game_id as "defense.game_id",
       gds.defense_assists as "defense.defense_assists",
       gds.created_at as "defense.created_at",
       gds.updated_at as "defense.updated_at",
       gds.deleted_at as "defense.deleted_at",
       gos.id as  "offense.id",
       gos.team_id as "offense.team_id",
       gos.game_id as "offense.game_id",
       gos.offense_at_bats as "offense.offense_at_bats",
       gos.created_at as "offense.created_at",
       gos.updated_at as "offense.updated_at",
       gos.deleted_at as "offense.deleted_at",
       gps.id as "pitching.id",
       gps.team_id as "pitching.team_id",
       gps.game_id as "pitching.game_id",
       gps.pitching_air_outs as "pitching.pitching_air_outs",
       gps.created_at as "pitching.created_at",
       gps.updated_at as "pitching.updated_at",
       gps.deleted_at as "pitching.deleted_at"
from game as g
         inner join GAME_DEFENSIVE_STATS as gds on g.id = gds.game_id
         inner join GAME_OFFENSIVE_STATS as gos on gds.game_id = gos.game_id and gds.team_id = gos.team_id
         inner join GAME_PITCHING_STATS as gps on gos.game_id = gps.game_id and gos.team_id = gps.team_id`

type (
	GameStats struct {
		ID           int        `json:"id,omitempty" db:"id"`
		AwayTeam     int        `json:"awayTeamId" db:"away_team_id"`
		HomeTeam     int        `json:"homeTeamId" db:"home_team_id"`
		GameDateTime time.Time  `json:"gameDateTime" db:"game_date_time"`
		Season       string     `json:"season,omitempty" db:"season"`
		CreatedAt    time.Time  `json:"createdAt" db:"created_at"`
		UpdatedAt    time.Time  `json:"updatedAt" db:"updated_at"`
		DeletedAt    *time.Time `json:"deletedAt" db:"deleted_at"`
	}
	OffenseStats struct {
		ID        int        `json:"ID,omitempty" db:"id"`
		Team      int        `json:"team,omitempty" db:"team_id"`
		Game      int        `json:"game,omitempty" db:"game_id"`
		AtBats    float32    `json:"atBats,omitempty" db:"offense_at_bats"`
		CreatedAt time.Time  `json:"createdAt" db:"created_at"`
		UpdatedAt time.Time  `json:"updatedAt" db:"updated_at"`
		DeletedAt *time.Time `json:"deletedAt" db:"deleted_at"`
	}
	DefenseStats struct {
		ID        int        `json:"ID,omitempty" db:"id"`
		Team      int        `json:"team,omitempty" db:"team_id"`
		Game      int        `json:"game,omitempty" db:"game_id"`
		Assists   float32    `json:"assists,omitempty" db:"defense_assists"`
		CreatedAt time.Time  `json:"createdAt" db:"created_at"`
		UpdateAt  time.Time  `json:"updateAT" db:"updated_at"`
		DeletedAt *time.Time `json:"deletedAt" db:"deleted_at"`
	}
	PitchingStats struct {
		ID        int        `json:"ID,omitempty" db:"id"`
		Team      int        `json:"team,omitempty" db:"team_id"`
		Game      int        `json:"game,omitempty" db:"game_id"`
		AirOuts   float32    `json:"airOuts" db:"pitching_air_outs"`
		CreatedAt time.Time  `json:"createdAt" db:"created_at"`
		UpdatedAt time.Time  `json:"updatedAt" db:"updated_at"`
		DeletedAt *time.Time `json:"deletedAt" db:"deleted_at"`
	}
	TeamStats struct {
		Game     GameStats     `db:"game"`
		Offense  OffenseStats  `db:"offense"`
		Defense  DefenseStats  `db:"defense"`
		Pitching PitchingStats `db:"pitching"`
	}
)

func main() {

	db, err := sqlx.Connect("postgres", "user=foo dbname=bar sslmode=disable")
	if err != nil {
		log.Fatalln(err)
	}

	db.MustExec(schema)

	tx := db.MustBegin()
	tx.MustExec("INSERT INTO team (id, name) VALUES ($1, $2)", 1, "Pittsburgh Pirates")
	tx.MustExec("INSERT INTO team (id, name) VALUES ($1, $2)", 2, "Baltimore Oriels")
	tx.MustExec("INSERT INTO game (id, home_team_id, away_team_id, game_date_time) VALUES ($1, $2, $3, $4)", 1, 1, 2, time.Now())
	tx.MustExec("INSERT INTO GAME_OFFENSIVE_STATS (team_id, game_id, offense_at_bats) VALUES ($1, $2, $3)", 1, 1, 27)
	tx.MustExec("INSERT INTO GAME_DEFENSIVE_STATS (team_id, game_id, DEFENSE_ASSISTS) VALUES ($1, $2, $3)", 1, 1, 8)
	tx.MustExec("INSERT INTO GAME_PITCHING_STATS (team_id, game_id, PITCHING_AIR_OUTS) VALUES ($1, $2, $3)", 1, 1, 16)

	err = tx.Commit()
	if err != nil {
		fmt.Println(err)
	}

	var game []GameStats
	err = db.Select(&game, "SELECT * FROM GAME")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("%#v\n", game)

	// Query the database, storing results in a []TeamStats
	var teamStats []TeamStats
	err = db.Select(&teamStats, getTeamStats)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("%#v\n", teamStats)

}

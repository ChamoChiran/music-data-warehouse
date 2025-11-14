IF OBJECT_ID ('bronze.lfm_top_tracks') IS NOT NULL
	DROP TABLE bronze.lfm_top_tracks;

CREATE TABLE bronze.lfm_top_tracks(
	chart_country VARCHAR(100) NOT NULL,
	chart_date DATE NOT NULL,

	track_rank INTEGER,
	track_name VARCHAR(500),
	track_duration VARCHAR(50),
	track_listeners BIGINT,
	track_mbid VARCHAR(200),
	track_url VARCHAR(500),

	artist_name VARCHAR(500),
	artist_mbid VARCHAR(200),
	artist_url VARCHAR(500),
	
	rec_load_date   DATETIME2 DEFAULT SYSDATETIME(),
);

IF OBJECT_ID ('bronze.lfm_top_artists') IS NOT NULL
	DROP TABLE bronze.lfm_top_artists;

CREATE TABLE bronze.lfm_top_artists(
    artist_name NVARCHAR(50) NOT NULL,
    artist_listeners INT NOT NULL,
    artist_mbid NVARCHAR(50) NULL,
    artist_url NVARCHAR(100),
    artist_streamable TINYINT NOT NULL,
    artist_rank TINYINT NOT NULL,

    chart_country NVARCHAR(50),
    chart_date NVARCHAR(50),

    rec_load_date DATETIME2 NULL DEFAULT SYSDATETIME()
);
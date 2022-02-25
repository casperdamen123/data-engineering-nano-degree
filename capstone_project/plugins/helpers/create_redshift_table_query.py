create_imdb_table = """
                    CREATE TABLE IF NOT EXISTS imdb_titles (
                        title_key TEXT,
                        primary_title TEXT,
                        original_title TEXT,
                        title_type TEXT, 
                        directors TEXT, 
                        writers TEXT, 
                        genres TEXT, 
                        has_directors BOOL, 
                        has_writers BOOL,
                        has_ended BOOL,
                        num_of_genres INT,
                        release_year INT,
                        endYear INT,
                        average_rating FLOAT,
                        num_votes INT,
                        runtime_minutes INT,
                        num_directors INT,
                        num_writers INT,                        
                        PRIMARY KEY (title_key)
                    );
                    """

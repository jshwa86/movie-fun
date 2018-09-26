package org.superbiz.moviefun.albums;

import org.apache.commons.lang.time.DateUtils;
import org.joda.time.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import javax.sql.DataSource;

import java.sql.Timestamp;
import java.util.Date;

@Configuration
@EnableAsync
@EnableScheduling
public class AlbumsUpdateScheduler {

    private static final long SECONDS = 1000;
    private static final long MINUTES = 60 * SECONDS;

    private final AlbumsUpdater albumsUpdater;
    private final JdbcTemplate jdbcTemplate;

    private final Logger logger = LoggerFactory.getLogger(getClass());


    public AlbumsUpdateScheduler(AlbumsUpdater albumsUpdater,
                                 DataSource datasource) {
        this.albumsUpdater = albumsUpdater;
        this.jdbcTemplate = new JdbcTemplate(datasource);
    }


    @Scheduled(initialDelay = 15 * SECONDS, fixedRate = 2 * MINUTES)
    public void run() {
        try {
            if(startAlbumSchedulerTask()){
                logger.debug("Starting albums update");
                albumsUpdater.update();

                logger.debug("Finished albums update");
            }else{
                logger.debug("Not enough time has passed for me to run the album update.");
            }

        } catch (Throwable e) {
            logger.error("Error while updating albums", e);
        }
    }

    private boolean startAlbumSchedulerTask(){

        //Tracking the last execution in table album_scheduler_task
        //Case1: Table is empty (no rows):
            //We should add the current timestamp to the table and then return true out of this method
        //Case2: Table has more than zero rows (note - assuming that the table only ever has 0 or 1 rows in this method)
            //Retrieve the only timestamp from the table, and see if it is within 2 minutes of right now.
                //If within 2 minutes of now: return false and do nothing;
                //else: Update the table with the current timestamp and return true

        //Reference SQL:
            //SELECT COUNT(*) FROM album_scheduler_task;
            //UPDATE album_scheduler_task SET started_at = '2008-01-01 00:00:01'
            //SELECT started_at FROM album_scheduler_task


        Timestamp timestampFromDb =
            jdbcTemplate.queryForObject("SELECT started_at FROM album_scheduler_task", (resultSet, i) -> {
                return resultSet.getTimestamp("started_at");
            });

        if(null == timestampFromDb){
            updateCurrentTimestampInAlbumDB();
            return true;
        }

        Date fromDatabasePlusTwoMinutes = DateUtils.addMinutes(timestampFromDb,2);
        Timestamp current = current();

        if (current.before(fromDatabasePlusTwoMinutes)){
            return false;
        }else {
            updateCurrentTimestampInAlbumDB();
            return true;
        }

    }

    private void updateCurrentTimestampInAlbumDB(){
        jdbcTemplate.update("UPDATE album_scheduler_task SET started_at = ?",current());
    }

    private Timestamp current(){
        return new Timestamp(System.currentTimeMillis());
    }

}

package org.superbiz.moviefun.albums;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.superbiz.moviefun.blobstore.Blob;
import org.superbiz.moviefun.blobstore.BlobStore;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.fasterxml.jackson.dataformat.csv.CsvSchema.ColumnType.NUMBER;
import static org.superbiz.moviefun.CsvUtils.readFromCsv;

@Service
public class AlbumsUpdater {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ObjectReader objectReader;
    private final BlobStore blobStore;
    private final AlbumsBean albumsBean;

    public AlbumsUpdater(BlobStore blobStore, AlbumsBean albumsBean) {
        this.blobStore = blobStore;
        this.albumsBean = albumsBean;

        CsvSchema schema = CsvSchema.builder()
            .addColumn("artist")
            .addColumn("title")
            .addColumn("year", NUMBER)
            .addColumn("rating", NUMBER)
            .build();

        objectReader = new CsvMapper().readerFor(Album.class).with(schema);
    }

    public void update() throws IOException {
        Optional<Blob> maybeBlob = blobStore.get("albums.csv");

        if (!maybeBlob.isPresent()) {
            logger.info("No albums.csv found when running AlbumsUpdater!");
            return;
        }

        List<Album> albumsFromCsv = readFromCsv(objectReader, maybeBlob.get().inputStream);
        List<Album> albumsFromDB = albumsBean.getAlbums();

        createNewAlbums(albumsFromCsv, albumsFromDB);
        deleteOldAlbums(albumsFromCsv, albumsFromDB);
        updateExistingAlbums(albumsFromCsv, albumsFromDB);
    }


    private void createNewAlbums(List<Album> albumsFromCsv, List<Album> albumsFromDB) {
        Stream<Album> albumsToCreate = albumsFromCsv
            .stream() //All albums from the csv file
            .filter( //Only retrieve albums from csv that match this -->
                    album -> albumsFromDB.stream() //ALL albums from the database
                            .noneMatch( //filter again on this ->
                                    album::isEquivalent //album from CSV is equivalent to album from DB
                            ));

        albumsToCreate.forEach(albumsBean::addAlbum);
    }

    private void deleteOldAlbums(List<Album> albumsFromCsv, List<Album> albumsFromDB) {
        Stream<Album> albumsToDelete = albumsFromDB
            .stream()
            .filter(album -> albumsFromCsv.stream().noneMatch(album::isEquivalent));

        albumsToDelete.forEach(albumsBean::deleteAlbum);
    }

    private void updateExistingAlbums(List<Album> albumsFromCsv, List<Album> albumsFromDB) {
        Stream<Album> albumsToUpdate = albumsFromCsv
            .stream()
            .map(album -> addIdToAlbumIfExists(albumsFromDB, album))
            .filter(Album::hasId);

        albumsToUpdate.forEach(albumsBean::updateAlbum);
    }

    private Album addIdToAlbumIfExists(List<Album> existingAlbums, Album album) {
        Optional<Album> maybeExisting = existingAlbums.stream().filter(album::isEquivalent).findFirst();
        maybeExisting.ifPresent(existing -> album.setId(existing.getId()));
        return album;
    }
}

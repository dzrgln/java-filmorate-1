package ru.yandex.practicum.filmorate.filmorate.storage;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.filmorate.filmorate.exceptions.ObjectNotFoundException;
import ru.yandex.practicum.filmorate.filmorate.exceptions.ValidationException;
import ru.yandex.practicum.filmorate.filmorate.model.Film;
import ru.yandex.practicum.filmorate.filmorate.model.Genre;
import ru.yandex.practicum.filmorate.filmorate.model.Mpa;
import ru.yandex.practicum.filmorate.filmorate.storage.interfaces.FilmStorage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
@Qualifier("FilmDbStorage")
public class FilmDbStorage implements FilmStorage {
    private final JdbcTemplate jdbcTemplate;

    private final String SQL_GET_FILMS = "SELECT F.FILM_ID,F.FILM_NAME, F.DESCRIPTION, F.RELEASE_DATE, F.DURATION " +
            "FROM FILM AS F";
    private final String SQL_GET_FILM_BY_ID = "SELECT F.FILM_ID,F.FILM_NAME, F.DESCRIPTION, F.RELEASE_DATE, F.DURATION " +
            "FROM FILM AS F " +
            "WHERE film_id = ?";
    private final String SQL_FOR_UPDATE_FILM = "update FILM set " +
            "FILM_NAME = ?, DESCRIPTION = ?, RELEASE_DATE = ?, DURATION = ?, RATING_ID = ? " +
            "where FILM_ID = ?";
    private  final String SQL_UPDATE_FILM = "SELECT FILM_ID FROM FILM WHERE FILM_ID = ?";
    private final String SQL_DELETE_FILM_FROM_FILMGENRE = "DELETE FROM FILM_GENRE WHERE FILM_ID = ?";
    private final String SQL_GET_MPA = "SELECT RATING_NAME FROM RATING WHERE RATING_ID = ?";
    private    final String SQL_GET_GENRE = "SELECT GENRE_NAME FROM GENRE WHERE GENRE_ID = ?";
    private  final String SQL_GET_MPA_BY_FILM = "SELECT R.RATING_ID, R.RATING_NAME\n" +
            "FROM FILM AS F\n" +
            "JOIN RATING R on F.RATING_ID = R.RATING_ID WHERE F.film_id = ?";
    private   final String SQL_GET_GENRE_BY_FILM = "SELECT FG.GENRE_ID, G.GENRE_NAME\n" +
            "      FROM FILM AS F\n" +
            "      JOIN FILM_GENRE as FG on F.FILM_ID = FG.FILM_ID\n" +
            "      JOIN GENRE as G on FG.GENRE_ID = G.GENRE_ID\n" +
            "      WHERE F.film_id = ?";
    private  final String SQL_FOR_ASSINMENT_ID_FOR_FILM = "SELECT FILM_ID\n" +
            "FROM Film\n" +
            "WHERE FILM_NAME = ?\n" +
            "    AND DESCRIPTION = ?\n" +
            "    AND RELEASE_DATE = ?\n" +
            "    AND DURATION = ?\n" +
            "    AND RATING_ID = ?";
    private    final String SQL_FOR_CHECKING_FILMGENRE = "SELECT GENRE_ID, FILM_ID FROM FILM_GENRE WHERE FILM_ID = ? and GENRE_ID = ?";

    private final String SQL_REMOVE_LIKE = "DELETE FROM LIKES WHERE USER_ID = ? AND FILM_ID = ?";
    private   final String SQL_GET_LIST_POPULAR_FILMS = "SELECT FILM_ID, COUNT(USER_ID)\n" +
            "FROM LIKES\n" +
            "GROUP BY FILM_ID\n" +
            "ORDER BY COUNT(USER_ID)\n" +
            "LIMIT ?";
    private    final String SQL_FOR_CHECKING_USER_ID = "SELECT * " +
            "FROM USER " +
            "WHERE USER_ID = ?";
    private   final String SQL_FOR_CHECKING_FILM_ID = "SELECT * " +
            "FROM FILM " +
            "WHERE FILM_ID = ?";

    public FilmDbStorage(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public List<Film> getFilms() {
        SqlRowSet filmRows = jdbcTemplate.queryForRowSet(SQL_GET_FILMS);
        List<Film> films = new ArrayList<>();
        while (filmRows.next()) {
            films.add(getFilmById(filmRows.getInt("FILM_ID")));
        }
        return films;
    }

    @Override
    public Film getFilmById(int id) {
        SqlRowSet filmRows = jdbcTemplate.queryForRowSet(SQL_GET_FILM_BY_ID, id);
        Film film = null;
        if (filmRows.next()) {
            film = new Film(
                    filmRows.getInt("FILM_ID"),
                    filmRows.getString("FILM_NAME"),
                    filmRows.getString("DESCRIPTION"),
                    filmRows.getDate("RELEASE_DATE").toLocalDate(),
                    filmRows.getInt("DURATION"),
                    getGenreByFilmId(id),
                    getMpaByFilmId(id)
            );
            log.info("Найден фильм: {}{}", film.getId(), film.getName());
        } else {
            log.info("Фильм с идентификатором: {} не найден", id);
            throw new ObjectNotFoundException("Указанного фильма не существует");
        }
        return film;
    }

    @Override
    public Film create(Film film) {
        if (film.getDescription().length() > 200) {
            throw new ValidationException("Описание фильма должно быть не более 200 символов");
        }
        if (film.getReleaseDate().isBefore(FIRST_FILM_DATE)) {
            throw new ValidationException("Дата релиза должна быть не раньше 28 декабря 1895 года");
        }
        SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(jdbcTemplate)
                .withTableName("FILM")
                .usingGeneratedKeyColumns("FILM_ID");
        simpleJdbcInsert.execute(film.toMap());
        assignIdForFilm(film);
        if (film.getGenres() != null) {
            createRowFILM_GENRE(film);
            assignGenre(film);
        }
        assignMpa(film);
        addFilmInLikeTable(film);
        return film;
    }

    @Override
    public Film update(Film film) {
        SqlRowSet idRows = jdbcTemplate.queryForRowSet(SQL_UPDATE_FILM, film.getId());
        if (idRows.next()) {
            jdbcTemplate.update(SQL_FOR_UPDATE_FILM
                    , film.getName()
                    , film.getDescription()
                    , film.getReleaseDate()
                    , film.getDuration()
                    , film.getMpa().getId()
                    , film.getId()
            );
            assignMpa(film);
            if (film.getGenres() != null) {
                createRowFILM_GENRE(film);
                assignGenre(film);
                List<Genre> genres = film.getGenres().stream().distinct().collect(Collectors.toList());
                film.setGenres(genres);
            } else {
                deleteGenreForFilm(film);
            }
        } else {
            throw new ObjectNotFoundException("Указанного фильма не существует");
        }
        return film;
    }

    @Override
    public void addLike(int userId, int filmId) {
        if (checkFilmId(filmId) && checkUserId(userId)) {
            SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(jdbcTemplate)
                    .withTableName("LIKES")
                    .usingGeneratedKeyColumns("LIKE_ID");
            Map<String, Object> like = new HashMap<>();
            like.put("USER_ID", userId);
            like.put("FILM_ID", filmId);
            simpleJdbcInsert.execute(like);
        } else {
            throw new ObjectNotFoundException(
                    String.format("Пользователя с id \"%s\" или фильма с id \"%s\" не существует."
                            , userId, filmId));
        }
    }

    @Override
    public void removeLike(int userId, int filmId) {
        if (checkFilmId(filmId) && checkUserId(userId)) {
            jdbcTemplate.update(SQL_REMOVE_LIKE, userId, filmId);

        } else {
            throw new ObjectNotFoundException(
                    String.format("Пользователя с id \"%s\" или фильма с id \"%s\" не существует."
                            , userId, filmId));
        }
    }

    private void assignMpa(Film film) {
        if (film.getMpa().getName() == null) {
            SqlRowSet mpaRow = jdbcTemplate.queryForRowSet(SQL_GET_MPA, film.getMpa().getId());
            if (mpaRow.next()) {
                film.getMpa().setName(mpaRow.getString("RATING_NAME"));
            }
        }
    }

    private void assignGenre(Film film) {
        for (Genre genre : film.getGenres()) {
            SqlRowSet mpaRow = jdbcTemplate.queryForRowSet(SQL_GET_GENRE, genre.getId());
            if (mpaRow.next()) {
                genre.setName(mpaRow.getString("GENRE_NAME"));
            }
        }
    }

    private void createRowFILM_GENRE(Film film) {
        if (!film.getGenres().isEmpty()) {
            SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(jdbcTemplate)
                    .withTableName("FILM_GENRE")
                    .usingGeneratedKeyColumns("FILM_GENRE_ID");
            List<Genre> genres = film.getGenres().stream().distinct().collect(Collectors.toList());
            deleteGenreForFilm(film);
            Map<String, Object> genre_film = new HashMap<>();
            for (Genre genre : genres) {
                if (!checkRatingFilmDuplicate(film, genre)) {
                    genre_film.put("FILM_ID", film.getId());
                    genre_film.put("GENRE_ID", genre.getId());
                    simpleJdbcInsert.execute(genre_film);
                }
            }
        } else {
            deleteGenreForFilm(film);
        }
    }

    private void deleteGenreForFilm(Film film) {
        jdbcTemplate.update(SQL_DELETE_FILM_FROM_FILMGENRE, film.getId());
    }


    private List<Genre> getGenreByFilmId(int id) {

        SqlRowSet genreRows = jdbcTemplate.queryForRowSet(SQL_GET_GENRE_BY_FILM, id);
        List<Genre> genres = new ArrayList<>();
        while (genreRows.next()) {
            Genre genre = new Genre(
                    genreRows.getInt("GENRE_ID"),
                    genreRows.getString("GENRE_NAME"));
            genres.add(genre);
        }
        SqlRowSet genreRows1 = jdbcTemplate.queryForRowSet(SQL_GET_GENRE_BY_FILM, id);
        if (!genreRows1.next()) {
            genres = null;
        }
        return genres;
    }

    private Mpa getMpaByFilmId(int id) {
        SqlRowSet mpaRows = jdbcTemplate.queryForRowSet(SQL_GET_MPA_BY_FILM, id);
        Mpa mpa = null;
        if (mpaRows.next()) {
            mpa = new Mpa(
                    mpaRows.getInt("RATING_ID"),
                    mpaRows.getString("RATING_NAME")
            );
        }
        return mpa;
    }

    private void assignIdForFilm(Film film) {
        if (film.getId() == 0) {
            SqlRowSet filmIdRows = jdbcTemplate.queryForRowSet(SQL_FOR_ASSINMENT_ID_FOR_FILM,
                    film.getName(),
                    film.getDescription(),
                    film.getReleaseDate(),
                    film.getDuration(),
                    film.getMpa().getId());
            if (filmIdRows.next()) {
                film.setId(filmIdRows.getInt("FILM_ID"));
            }
        }
    }

    private void addFilmInLikeTable(Film film) {
        SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(jdbcTemplate)
                .withTableName("LIKES")
                .usingGeneratedKeyColumns("LIKE_ID");
        Map<String, Object> map = new HashMap<>();
        map.put("FILM_ID", film.getId());
        simpleJdbcInsert.execute(map);
    }

    private boolean checkRatingFilmDuplicate(Film film, Genre genre) {
        SqlRowSet filmGenreRows = jdbcTemplate.queryForRowSet(SQL_FOR_CHECKING_FILMGENRE, film.getId(), genre.getId());
        return filmGenreRows.next();
    }

    public List<Film> getListPopularFilms(int limit) {
        List<Film> topFilms = new ArrayList<>();
        SqlRowSet likeFilms = jdbcTemplate.queryForRowSet(SQL_GET_LIST_POPULAR_FILMS, limit);
        while (likeFilms.next()) {
            topFilms.add(getFilmById(likeFilms.getInt("FILM_ID")));
        }
        return topFilms;
    }

    private boolean checkUserId(int userId) {
        SqlRowSet userRows = jdbcTemplate.queryForRowSet(SQL_FOR_CHECKING_USER_ID, userId);
        return userRows.next();
    }

    private boolean checkFilmId(int filmId) {
        SqlRowSet filmRows = jdbcTemplate.queryForRowSet(SQL_FOR_CHECKING_FILM_ID, filmId);
        return filmRows.next();
    }
}

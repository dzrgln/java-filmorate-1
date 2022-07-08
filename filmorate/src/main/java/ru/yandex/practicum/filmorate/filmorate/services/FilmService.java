package ru.yandex.practicum.filmorate.filmorate.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.filmorate.filmorate.exceptions.ObjectNotFoundException;
import ru.yandex.practicum.filmorate.filmorate.model.Film;
import ru.yandex.practicum.filmorate.filmorate.storage.interfaces.FilmStorage;
import ru.yandex.practicum.filmorate.filmorate.storage.interfaces.UserStorage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class FilmService {
    private final FilmStorage filmStorage;
    private final UserStorage userStorage;
    private final JdbcTemplate jdbcTemplate;
    final String SQL_REMOVE_LIKE = "DELETE FROM LIKES WHERE USER_ID = ? AND FILM_ID = ?";
    final String SQL_GET_LIST_POPULAR_FILMS = "SELECT FILM_ID, COUNT(USER_ID)\n" +
            "FROM LIKES\n" +
            "GROUP BY FILM_ID\n" +
            "ORDER BY COUNT(USER_ID)\n" +
            "LIMIT ?";
    final String SQL_FOR_CHECKING_USER_ID = "SELECT * " +
            "FROM USER " +
            "WHERE USER_ID = ?";
    final String SQL_FOR_CHECKING_FILM_ID = "SELECT * " +
            "FROM FILM " +
            "WHERE FILM_ID = ?";

    @Autowired
    public FilmService(
            @Qualifier("FilmDbStorage") FilmStorage filmStorage, UserStorage userStorage, JdbcTemplate jdbcTemplate) {
        this.filmStorage = filmStorage;
        this.userStorage = userStorage;
        this.jdbcTemplate = jdbcTemplate;
    }

    public void addLike(Integer userId, Integer filmId) {
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

    public void removeLike(Integer userId, Integer filmId) {
        if (checkFilmId(filmId) && checkUserId(userId)) {
            jdbcTemplate.update(SQL_REMOVE_LIKE, userId, filmId);

        } else {
            throw new ObjectNotFoundException(
                    String.format("Пользователя с id \"%s\" или фильма с id \"%s\" не существует."
                            , userId, filmId));
        }
    }

    public List<Film> getListPopularFilms(int limit) {
        List<Film> topFilms = new ArrayList<>();
        SqlRowSet likeFilms = jdbcTemplate.queryForRowSet(SQL_GET_LIST_POPULAR_FILMS, limit);
        while (likeFilms.next()) {
            topFilms.add(filmStorage.getFilmById(likeFilms.getInt("FILM_ID")));
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

package ru.yandex.practicum.filmorate.filmorate.storage;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.filmorate.filmorate.exceptions.ObjectNotFoundException;
import ru.yandex.practicum.filmorate.filmorate.model.Relationship;
import ru.yandex.practicum.filmorate.filmorate.model.User;
import ru.yandex.practicum.filmorate.filmorate.storage.interfaces.UserStorage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
@Component
@Qualifier("UserDbStorage")
public class UserDbStorage implements UserStorage {
    private final JdbcTemplate jdbcTemplate;
    final String SQL_GET_USERS = "SELECT * " +
            "FROM USER";
    final  String SQL_GET_USERS_BY_ID = "SELECT * " +
            "FROM USER " +
            "WHERE USER_ID = ?";
    final String SQL_UPDATE_USER = "SELECT USER_ID FROM USER WHERE USER_ID = ?";
    final String SQLUPDATE = "update USER set " +
            "USER_NAME = ?, EMAIL = ?, LOGIN = ?, BIRTHDAY = ? " +
            "where USER_ID = ?";
    final String SQL_GET_FRIENDS_FOR_USER_BY_ID = "SELECT R.FRIEND_ID, STATUS_NAME\n" +
            "    FROM USER AS U\n" +
            "JOIN RELATIONSHIP AS R ON U.USER_ID = R.USER_ID\n" +
            "JOIN STATUS S on R.STATUS_ID = S.STATUS_ID\n" +
            "WHERE STATUS_NAME = 'confirmed' and U.USER_ID = ?;";
    final String SQL_GET_FAVORITE_FILMS_FOR_USER_BE_ID = "SELECT L.FILM_ID\n" +
            "    FROM USER AS U\n" +
            "JOIN LIKES AS L ON U.USER_ID = L.USER_ID\n" +
            "WHERE U.USER_ID = ?;";
    final String SQL_FOR_ASSIGNMENT_ID_FOR_USER = "SELECT USER_ID\n" +
            "FROM User\n" +
            "WHERE USER_NAME = ?\n" +
            "    AND EMAIL = ?\n" +
            "    AND LOGIN = ?\n" +
            "    AND BIRTHDAY = ?\n";

    public UserDbStorage(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public List<User> getUsers() {
        SqlRowSet userRows = jdbcTemplate.queryForRowSet(SQL_GET_USERS);
        List<User> users = new ArrayList<>();
        while (userRows.next()) {
            users.add(getUserById(userRows.getInt("USER_ID")));
        }
        return users;
    }

    @Override
    public User getUserById(int id) {
        SqlRowSet userRows = jdbcTemplate.queryForRowSet(SQL_GET_USERS_BY_ID, id);
        User user = null;
        if (userRows.next()) {
            user = new User(
                    userRows.getInt("USER_ID"),
                    userRows.getString("USER_NAME"),
                    userRows.getString("EMAIL"),
                    userRows.getString("LOGIN"),
                    userRows.getDate("BIRTHDAY").toLocalDate(),
                    getFriendsByUserId(id),
                    getFavoriteFilmsByUserId(id)
            );

            log.info("Найден пользователь: {}{}", user.getId(), user.getName());
        } else {
            log.info("Пользователь с идентификатором: {} не найден", id);
            throw new ObjectNotFoundException("Указанного пользователя не существует");
        }
        return user;
    }

    @Override
    public User create(User user) {
        SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(jdbcTemplate)
                .withTableName("USER")
                .usingGeneratedKeyColumns("USER_ID");
        simpleJdbcInsert.execute(user.toMap());
        assignIdForUser(user);
        if (user.getName().equals("")) {
            user.setName(user.getLogin());
            update(user);
        }
        return user;
    }

    @Override
    public User update(User user) {
        SqlRowSet idRows = jdbcTemplate.queryForRowSet(SQL_UPDATE_USER, user.getId());
        if (idRows.next()) {
            jdbcTemplate.update(SQLUPDATE
                    , user.getName()
                    , user.getEmail()
                    , user.getLogin()
                    , user.getBirthday()
                    , user.getId()
            );
        } else {
            throw new ObjectNotFoundException("Указанного пользователя не существует");
        }
        return user;
    }


    private void createRowRelationship(User user) {
        SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(jdbcTemplate)
                .withTableName("RELATIONSHIP")
                .usingGeneratedKeyColumns("RELATIONSHIP_ID");
        for (int id : user.getFriends()) {
            Relationship relationship = new Relationship(user.getId(), id);
            simpleJdbcInsert.execute(relationship.toMap());
        }
    }

    private Set<Integer> getFriendsByUserId(int id) {
        SqlRowSet friendRows = jdbcTemplate.queryForRowSet(SQL_GET_FRIENDS_FOR_USER_BY_ID, id);
        Set<Integer> setId = new HashSet<>();
        while (friendRows.next()) {
            setId.add(friendRows.getInt("FRIEND_ID"));
        }
        return setId;
    }

    private Set<Integer> getFavoriteFilmsByUserId(int id) {
        SqlRowSet favoriteFilmsRows = jdbcTemplate.queryForRowSet(SQL_GET_FAVORITE_FILMS_FOR_USER_BE_ID, id);
        Set<Integer> setId = new HashSet<>();
        while (favoriteFilmsRows.next()) {
            setId.add(favoriteFilmsRows.getInt("FILM_ID"));
        }
        return setId;
    }

    private void assignIdForUser(User user) {
        if (user.getId() == 0) {
            SqlRowSet userIdRows = jdbcTemplate.queryForRowSet(SQL_FOR_ASSIGNMENT_ID_FOR_USER,
                    user.getName(),
                    user.getEmail(),
                    user.getLogin(),
                    user.getBirthday());
            if (userIdRows.next()) {
                user.setId(userIdRows.getInt("USER_ID"));
            }
        }
    }

}

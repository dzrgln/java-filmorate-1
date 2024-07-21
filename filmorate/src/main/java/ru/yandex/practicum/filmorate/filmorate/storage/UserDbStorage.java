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

import java.util.*;

@Slf4j
@Component
@Qualifier("UserDbStorage")
public class UserDbStorage implements UserStorage {
    private final JdbcTemplate jdbcTemplate;
    private     final String SQL_GET_USERS = "SELECT * " +
            "FROM USER";
    private   final  String SQL_GET_USERS_BY_ID = "SELECT * " +
            "FROM USER " +
            "WHERE USER_ID = ?";
    private    final String SQL_UPDATE_USER = "SELECT USER_ID FROM USER WHERE USER_ID = ?";
    private   final String SQLUPDATE = "update USER set " +
            "USER_NAME = ?, EMAIL = ?, LOGIN = ?, BIRTHDAY = ? " +
            "where USER_ID = ?";
    private   final String SQL_GET_FRIENDS_FOR_USER_BY_ID = "SELECT R.FRIEND_ID, STATUS_NAME\n" +
            "    FROM USER AS U\n" +
            "JOIN RELATIONSHIP AS R ON U.USER_ID = R.USER_ID\n" +
            "JOIN STATUS S on R.STATUS_ID = S.STATUS_ID\n" +
            "WHERE STATUS_NAME = 'confirmed' and U.USER_ID = ?;";
    private    final String SQL_GET_FAVORITE_FILMS_FOR_USER_BE_ID = "SELECT L.FILM_ID\n" +
            "    FROM USER AS U\n" +
            "JOIN LIKES AS L ON U.USER_ID = L.USER_ID\n" +
            "WHERE U.USER_ID = ?;";
    private   final String SQL_FOR_ASSIGNMENT_ID_FOR_USER = "SELECT USER_ID\n" +
            "FROM User\n" +
            "WHERE USER_NAME = ?\n" +
            "    AND EMAIL = ?\n" +
            "    AND LOGIN = ?\n" +
            "    AND BIRTHDAY = ?\n";
    private   final String SQL_UPDATE_RELATIONSHIP2 = "update RELATIONSHIP set STATUS_ID = 2 where USER_ID = ? AND FRIEND_ID = ?";
    private    final String SQL_FOR_CHECKING_FRIENDS = "SELECT * FROM RELATIONSHIP WHERE USER_ID = ? AND FRIEND_ID = ?";
    private   final String SQL_DELETE_RELATIONSHIP = "DELETE FROM RELATIONSHIP WHERE USER_ID = ? AND FRIEND_ID = ?";
    private   final String SQL_UPDATE_RELATIONSHIP1 = "update RELATIONSHIP set STATUS_ID = 1 where FRIEND_ID = ? AND USER_ID = ?";
    private   final String SQL_GET_MUTAL_FRIENDS = "SELECT FRIEND_ID FROM RELATIONSHIP WHERE USER_ID=?\n" +
            "INTERSECT\n" +
            "SELECT FRIEND_ID FROM RELATIONSHIP WHERE USER_ID=?";
    private    final String SQL_CHECKING = "SELECT * " +
            "FROM USER " +
            "WHERE USER_ID = ?";

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

    @Override
    public List<User> getMutualFriends(int userId, int friendId) {
        List<User> list = new ArrayList<>();
        if (checkUserId(userId) && checkUserId(friendId)) {
            SqlRowSet friends = jdbcTemplate.queryForRowSet(SQL_GET_MUTAL_FRIENDS, userId, friendId);
            while (friends.next()) {
                list.add(getUserById(friends.getInt("FRIEND_ID")));
            }
        } else {
            throw new ObjectNotFoundException(
                    String.format("Пользователя с id \"%s\" или с id \"%s\" не существует."
                            , userId, friendId));
        }
        return list;
    }

    @Override
    public List<User> getFriendsList(int userId) {
        List<User> list = new ArrayList<>();
        if (checkUserId(userId)) {
            final String SQL = "SELECT FRIEND_ID FROM RELATIONSHIP WHERE USER_ID = ?";
            SqlRowSet friends = jdbcTemplate.queryForRowSet(SQL, userId);
            while (friends.next()) {
                list.add(getUserById(friends.getInt("FRIEND_ID")));
            }
        } else {
            throw new ObjectNotFoundException(
                    String.format("Пользователя с id \"%s\"не существует.", userId));
        }
        return list;
    }

    @Override
    public void removeFriend(int userId, int friendId) {
        if (checkUserId(userId) && checkUserId(friendId)) {
            jdbcTemplate.update(SQL_DELETE_RELATIONSHIP, userId, friendId);
            jdbcTemplate.update(SQL_UPDATE_RELATIONSHIP1, friendId, userId);
        } else {
            throw new ObjectNotFoundException(
                    String.format("Пользователя с id \"%s\" или с id \"%s\" не существует."
                            , userId, friendId));
        }

    }

    @Override
    public void addFriend(int userId, int friendId) {
        if (checkUserId(userId) && checkUserId(friendId)) {
            SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(jdbcTemplate)
                    .withTableName("RELATIONSHIP")
                    .usingGeneratedKeyColumns("RELATIONSHIP_ID");
            Map<String, Object> friend = new HashMap<>();
            friend.put("USER_ID", userId);
            friend.put("FRIEND_ID", friendId);
            friend.put("STATUS_ID", 1);
            simpleJdbcInsert.execute(friend);
        } else {
            throw new ObjectNotFoundException(
                    String.format("Пользователя с id \"%s\" или с id \"%s\" не существует."
                            , userId, friendId));
        }
        if (isFriends(userId, friendId)) {

            jdbcTemplate.update(SQL_UPDATE_RELATIONSHIP1, userId, friendId);
            jdbcTemplate.update(SQL_UPDATE_RELATIONSHIP1, friendId, userId);
        }
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

    private boolean isFriends(Integer userId, Integer friendId) {
        SqlRowSet userFriendRow = jdbcTemplate.queryForRowSet(SQL_FOR_CHECKING_FRIENDS, userId, friendId);
        SqlRowSet friendUserRow = jdbcTemplate.queryForRowSet(SQL_FOR_CHECKING_FRIENDS, friendId, userId);
        return userFriendRow.next() && friendUserRow.next();
    }

    private boolean checkUserId(int userId) {
        SqlRowSet userRows = jdbcTemplate.queryForRowSet(SQL_CHECKING, userId);
        return userRows.next();
    }

}

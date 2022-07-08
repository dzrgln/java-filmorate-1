package ru.yandex.practicum.filmorate.filmorate.services;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.filmorate.filmorate.exceptions.ObjectNotFoundException;
import ru.yandex.practicum.filmorate.filmorate.model.User;
import ru.yandex.practicum.filmorate.filmorate.storage.interfaces.UserStorage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class UserService {
    private final UserStorage userStorage;
    private final JdbcTemplate jdbcTemplate;
    final String SQL_UPDATE_RELATIONSHIP2 = "update RELATIONSHIP set STATUS_ID = 2 where USER_ID = ? AND FRIEND_ID = ?";
    final String SQL_FOR_CHECKING_FRIENDS = "SELECT * FROM RELATIONSHIP WHERE USER_ID = ? AND FRIEND_ID = ?";
    final String SQL_DELETE_RELATIONSHIP = "DELETE FROM RELATIONSHIP WHERE USER_ID = ? AND FRIEND_ID = ?";
    final String SQL_UPDATE_RELATIONSHIP1 = "update RELATIONSHIP set STATUS_ID = 1 where FRIEND_ID = ? AND USER_ID = ?";
    final String SQL_GET_MUTAL_FRIENDS = "SELECT FRIEND_ID FROM RELATIONSHIP WHERE USER_ID=?\n" +
            "INTERSECT\n" +
            "SELECT FRIEND_ID FROM RELATIONSHIP WHERE USER_ID=?";
    final String SQL_CHECKING = "SELECT * " +
            "FROM USER " +
            "WHERE USER_ID = ?";

    @Autowired
    public UserService(@Qualifier("UserDbStorage") UserStorage userStorage, JdbcTemplate jdbcTemplate) {
        this.userStorage = userStorage;
        this.jdbcTemplate = jdbcTemplate;
    }

    public void addFriend(Integer userId, Integer friendId) {
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

    private boolean isFriends(Integer userId, Integer friendId) {
        SqlRowSet userFriendRow = jdbcTemplate.queryForRowSet(SQL_FOR_CHECKING_FRIENDS, userId, friendId);
        SqlRowSet friendUserRow = jdbcTemplate.queryForRowSet(SQL_FOR_CHECKING_FRIENDS, friendId, userId);
        return userFriendRow.next() && friendUserRow.next();
    }

    public void removeFriend(Integer userId, Integer friendId) {
        if (checkUserId(userId) && checkUserId(friendId)) {
            jdbcTemplate.update(SQL_DELETE_RELATIONSHIP, userId, friendId);
            jdbcTemplate.update(SQL_UPDATE_RELATIONSHIP1, friendId, userId);
        } else {
            throw new ObjectNotFoundException(
                    String.format("Пользователя с id \"%s\" или с id \"%s\" не существует."
                            , userId, friendId));
        }
    }

    public List<User> getFriendsList(Integer userId) {
        List<User> list = new ArrayList<>();
        if (checkUserId(userId)) {
            final String SQL = "SELECT FRIEND_ID FROM RELATIONSHIP WHERE USER_ID = ?";
            SqlRowSet friends = jdbcTemplate.queryForRowSet(SQL, userId);
            while (friends.next()) {
                list.add(userStorage.getUserById(friends.getInt("FRIEND_ID")));
            }
        } else {
            throw new ObjectNotFoundException(
                    String.format("Пользователя с id \"%s\"не существует.", userId));
        }
        return list;
    }

    public List<User> getMutualFriends(Integer userId, Integer friendId) {
        List<User> list = new ArrayList<>();
        if (checkUserId(userId) && checkUserId(friendId)) {
            SqlRowSet friends = jdbcTemplate.queryForRowSet(SQL_GET_MUTAL_FRIENDS, userId, friendId);
            while (friends.next()) {
                list.add(userStorage.getUserById(friends.getInt("FRIEND_ID")));
            }
        } else {
            throw new ObjectNotFoundException(
                    String.format("Пользователя с id \"%s\" или с id \"%s\" не существует."
                            , userId, friendId));
        }
        return list;
    }

    private boolean checkUserId(int userId) {
        SqlRowSet userRows = jdbcTemplate.queryForRowSet(SQL_CHECKING, userId);
        return userRows.next();
    }

}
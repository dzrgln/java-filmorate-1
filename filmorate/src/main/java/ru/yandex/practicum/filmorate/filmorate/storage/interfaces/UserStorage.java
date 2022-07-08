package ru.yandex.practicum.filmorate.filmorate.storage.interfaces;

import ru.yandex.practicum.filmorate.filmorate.model.User;

import java.util.List;

public interface UserStorage {
    public List<User> getUsers();

    public User getUserById(int id);

    public User create(User user);

    public User update (User user);

    List<User> getMutualFriends(int userId, int friendId);

    List<User> getFriendsList(int userId);

    void removeFriend(int userId, int friendId);

    void addFriend(int userId, int friendId);
}

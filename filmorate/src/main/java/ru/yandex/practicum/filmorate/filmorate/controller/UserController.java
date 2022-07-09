package ru.yandex.practicum.filmorate.filmorate.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.filmorate.filmorate.model.User;
import ru.yandex.practicum.filmorate.filmorate.services.UserService;
import ru.yandex.practicum.filmorate.filmorate.storage.interfaces.UserStorage;

import java.util.List;

@RestController
@Slf4j
@RequestMapping("/users")
public class UserController {
    private final UserStorage userStorage;
    private final UserService userService;

    @Autowired
    public UserController(@Qualifier("UserDbStorage")UserStorage userStorage, UserService userService) {
        this.userStorage = userStorage;
        this.userService = userService;
    }

    @GetMapping
    public List<User> getUsers() {
        log.info("Получен GET-запрос к эндпоинту /users");
        return userStorage.getUsers();
    }

    @GetMapping("/{userId}")
    public User getUserById(@PathVariable("userId") int userId){
        return userStorage.getUserById(userId);
    }

    @PostMapping
    public User create(@Validated @RequestBody User user) {
        log.info("Создан объект '{}'", user);
        return userStorage.create(user);
    }

    @PutMapping
    public User update(@Validated @RequestBody User user) {
        log.info("Обновлен объект '{}'", user);
        return userStorage.update(user);
    }

    @PutMapping("/{userId}/friends/{friendId}")
    public void addFriend(@PathVariable("userId") int userId, @PathVariable("friendId") int friendId){
        userStorage.addFriend(userId, friendId);
    }

    @DeleteMapping("/{userId}/friends/{friendId}")
    public void removeFriend(@PathVariable("userId") int userId, @PathVariable("friendId") int friendId){
        userStorage.removeFriend(userId, friendId);
    }

    @GetMapping("/{userId}/friends")
    public List<User> getFriendsList(@PathVariable("userId") int userId){
        return userStorage.getFriendsList(userId);
    }

    @GetMapping("/{userId}/friends/common/{friendId}")
    public List<User> getMutualFriends(@PathVariable("userId") int userId, @PathVariable("friendId") int friendId){
        return userStorage.getMutualFriends(userId, friendId);
    }

}

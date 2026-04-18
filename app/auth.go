package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"slices"
)

type UserAuth struct {
	username  string
	Passwords []string
	Flags     []string
}

var user *UserAuth = nil

func NewUserAuth() *UserAuth {
	if user != nil {
		return user
	}
	user = &UserAuth{}
	user.username = "default"
	user.Passwords = make([]string, 0)
	user.Flags = make([]string, 0)
	user.Flags = append(user.Flags, "nopass")
	return user
}

func GetUser() *UserAuth {
	return user
}

func (u *UserAuth) authenticateOnStartup() bool {

	return slices.Contains(u.Flags, "nopass")
}

func (u *UserAuth) setPassword(password string) *UserAuth {

	hash := sha256.Sum256([]byte(password))
	hashString := hex.EncodeToString(hash[:])

	u.Passwords = append(u.Passwords, hashString)

	var updatedFlags []string
	for _, flag := range u.Flags {
		if flag != "nopass" {
			updatedFlags = append(updatedFlags, flag)
		}
	}
	u.Flags = updatedFlags
	return u
}

func (u *UserAuth) setNoPass() {
	u.Passwords = make([]string, 0)

	hasNopass := false
	for _, flag := range u.Flags {
		if flag == "nopass" {
			hasNopass = true
		}
	}
	if !hasNopass {
		u.Flags = append(u.Flags, "nopass")
	}
}

func (u *UserAuth) authenticate(username string, password string) bool {

	if u.username != username {
		return false
	}

	inputHash := sha256.Sum256([]byte(password))
	inputHashString := hex.EncodeToString(inputHash[:])

	for _, storedHash := range u.Passwords {
		// subtle.ConstantTimeCompare prevents timing attacks
		if subtle.ConstantTimeCompare([]byte(storedHash), []byte(inputHashString)) == 1 {
			return true
		}
	}

	return false
}

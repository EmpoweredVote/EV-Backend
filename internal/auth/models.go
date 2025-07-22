package auth

import "time"

type Session struct {
	SessionID string 	`gorm:"primaryKey" json:"-"`
	UserID	  string 	`gorm:"not null;unique" json:"-"`
	ExpiresAt time.Time `gorm:"not null"`
}

type User struct {
	UserID			string	`gorm:"primaryKey" json:"user_id"`
	Username 		string  `json:"username"`
	Password 		string	`json:"password" gorm:"-"`
	HashedPassword  string  `json:"-"`
	Role			string  `gorm:"default:'user'"`
	AccountType		string  `gorm:"default:'informed'" json:"account_type"`
	ProfilePicURL  	string `json:"profile_pic_url"`
	Session			Session	`gorm:"foreignKey:UserID"`
}
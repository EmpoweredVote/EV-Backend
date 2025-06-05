package models

type User struct {
	UserID			string	`gorm:"primaryKey" json:"user_id"`
	Username 		string  `json:"username"`
	Password 		string	`json:"password" gorm:"-"`
	HashedPassword  string  `json:"-"`
	Session			Session	`gorm:"foreignKey:UserID"`
}
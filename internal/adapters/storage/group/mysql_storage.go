package group

import (
	"queue_balancer/internal/domain/group"
)

type GroupMysqlStorage struct {
}

func (gms *GroupMysqlStorage) GetOne(groupId int) (group.Group, error) {

	/*

		Тут будет что-то вроде
		SELECT group_id, user_id FROM `groups` WHERE group_id = groupId

	*/

	return group.Group{
		GroupId: groupId,
		UserId:  1,
	}, nil
}

func (gms *GroupMysqlStorage) GetMany(offsetId int, limit int) ([]group.Group, error) {

	/*

		Тут будет что-то вроде

		SELECT
			`groups`.`group_id`,
			`groups`.`user_id`,
			COUNT(`bots`.`bot_id`) as bots_count
		FROM `groups`
		LEFT JOIN `bots` ON `bots`.`group_id` = `groups`.`group_id`
		WHERE `groups`.`group_id` > offsetId
		GROUP BY `groups`.`group_id`
		HAVING bots_count > 0
		LIMIT limit

		чтобы отфильтровать только те сообщества, в которых есть боты


	*/

	return []group.Group{}, nil
}

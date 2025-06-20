package seeds

func SeedAll() error {
	if err := SeedCategories(); err != nil {
		return err
	}
	if err := SeedTopics(); err != nil {
		return err
	}
	if err := SeedTopicCategories(); err != nil {
		return err
	}
	return nil
}
service SearchBirdService {
  string get(1: string key) throws(1: SearchbirdException ex)

  void put(1: string key, 2: string value)
}
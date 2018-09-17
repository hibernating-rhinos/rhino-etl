process test_input_timeout:
    input "etltest_dsl", Command = "SELECT id, name as firstname, '' as lastname, email  FROM Users", Timeout = 60
    sqlBulkInsert "etltest_dsl", "People", TableLock = true :
        map "firstname"
        map "lastname"
        map "email"
        map "userid", "id", int

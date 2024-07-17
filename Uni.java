package src.main.java;

import java.sql.*;

public class Uni {

    private static final String PROTOCOL = "jdbc:postgresql://";        // URL-prefix
    private static final String DRIVER = "org.postgresql.Driver";       // Driver name
    private static final String URL_LOCALE_NAME = "localhost/";         // ваш компьютер + порт по умолчанию

    private static final String DATABASE_NAME = "uni";          // fightEvilDB uni FIXME имя базы

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";                  // FIXME имя пользователя
    public static final String DATABASE_PASS = "postgres";              // FIXME пароль базы данных

    public static void main(String[] args) {

        // проверка возможности подключения
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        // попытка открыть соединение с базой данных, которое java-закроет перед выходом из try-with-resources
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            //TODO show all tables
            //getVillains(connection); System.out.println();
            getPosition(connection); System.out.println();

            //getContracts(connection); System.out.println();

            // TODO show with param
            //getVillainNamed(connection, "Грю", false); System.out.println();// возьмем всех и найдем перебором
            //getVillainNamed(connection, "Грю", true); System.out.println(); // тоже самое сделает БД
            //getVillainMinions(connection, "Грю"); System.out.println();

            // TODO correction
            addPosition(connection, "преподаватель"); System.out.println();
            addSpeciality(connection, "Разработка ПО");
            addAuditoriums(connection, "1309");
            //correctMinion(connection, "Карл", 4); System.out.println();
            //removeMinion(connection, "Карл"); System.out.println();

        } catch (SQLException e) {
            // При открытии соединения, выполнении запросов могут возникать различные ошибки
            // Согласно стандарту SQL:2008 в ситуациях нарушения ограничений уникальности (в т.ч. дублирования данных) возникают ошибки соответствующие статусу (или дочерние ему): SQLState 23000 - Integrity Constraint Violation
            if (e.getSQLState().startsWith("23")){
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }

    public static void checkDriver () {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB () {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }
    // создание должности, специальности и аудитории, факультета, студента, преподавателя, кафедры и группы
    private static void addPosition(Connection connection, String name) throws SQLException{
        if(name == null || name.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO position(name) VALUES (?) returning id;", Statement.RETURN_GENERATED_KEYS);    // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, name);    // "безопасное" добавление имени

        int count =
                statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        ResultSet rs = statement.getGeneratedKeys(); // прочитать запрошенные данные от БД
        if (rs.next()) { // прокрутить к первой записи, если они есть
            System.out.println("Идентификатор должности " + rs.getInt(1));
        }

        System.out.println("INSERTed " + count + " position");
        getPosition(connection);

    }
    private static void addSpeciality(Connection connection, String name) throws SQLException{
        if(name == null || name.isBlank()) return;
        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO speciality(name) VALUES (?) returning id;", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, name);
        int count =
                statement.executeUpdate();
        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) { // прокрутить к первой записи, если они есть
            System.out.println("Идентификатор специальности " + rs.getInt(1));
        }
        System.out.println("INSERTed " + count + " speciality");
        getSpeciality(connection);

    }
    private static void addAuditoriums(Connection connection, String name) throws SQLException{
        if(name == null || name.isBlank()) return;
        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO auditoriums(name) VALUES (?) returning id;", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, name);
        int count =
                statement.executeUpdate();
        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) { // прокрутить к первой записи, если они есть
            System.out.println("Идентификатор аудитории" + rs.getInt(1));
        }
        System.out.println("INSERTed " + count + " auditoriums");
        getAuditoriums(connection);

    }
    private static void addFaculty(Connection connection, String name, int specialityId, int positionId) throws SQLException {
        if (name == null || name.isBlank()) return;
        if (specialityId <= 0) return;
        if (positionId <= 0) return;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO faculty(name, speciality_id, position_id) VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, name);
        statement.setInt(2, specialityId);
        statement.setInt(3, positionId);

        int count = statement.executeUpdate();
        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            int id = rs.getInt(1);
            System.out.println("Добавлен факультет с идентификатором " + id);
        }
        System.out.println("Добавлено " + count + " факультетов");
        getFaculty(connection);
    }
    private static void addStudent(Connection connection, String firstName, String lastName, Date birthDate, Integer specialityId) throws SQLException {
        if (firstName == null || firstName.isBlank() || lastName == null || lastName.isBlank() || birthDate == null) {
            System.out.println("Имя, фамилия и дата рождения студента не должны быть пустыми");
            return;
        }

        if (specialityId != null) {
            System.out.println("Специальность с идентификатором " + specialityId + " не существует");
            return;
        }

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO students(firstName, lastName, birthDate, speciality_id) VALUES (?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, firstName);
        statement.setString(2, lastName);
        statement.setDate(3, birthDate);
        statement.setInt(4, specialityId == null ? null : specialityId);

        int count = statement.executeUpdate();

        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            int id = rs.getInt(1);
            System.out.println("Добавлен студент с идентификатором " + id);
        }

        System.out.println("Добавлено " + count + " студентов");
        getStudent(connection);
    }
    private static void addTeacher(Connection connection, String firstName, String lastName, String education, Integer workExperience, Integer positionId) throws SQLException {
        if (firstName == null || firstName.isBlank() || lastName == null || lastName.isBlank() || education == null || education.isBlank()) {
            System.out.println("Имя, фамилия и образование учителя не должны быть пустыми");
            return;
        }
        if (positionId != null) {
            System.out.println("Должность с идентификатором " + positionId + " не существует");
            return;
        }
        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO teachers(firstName, lastName, education, workExperience, position_id) VALUES (?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, firstName);
        statement.setString(2, lastName);
        statement.setString(3, education);
        statement.setInt(4, workExperience == null ? null : workExperience);
        statement.setInt(5, positionId == null ? null : positionId);

        int count = statement.executeUpdate();

        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            int id = rs.getInt(1);
            System.out.println("Добавлен учитель с идентификатором " + id);
        }

        System.out.println("Добавлено " + count + " учителей");
        getTeacher(connection);
    }

    private static void addDepartment(Connection connection, String name, Integer facultyId) throws SQLException {
        if (name == null || name.isBlank()) {
            System.out.println("Название отделения не может быть пустым");
            return;
        }
        if (facultyId != null) {
            System.out.println("Факультет с идентификатором " + facultyId + " не существует");
            return;
        }
        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO departments(name, faculty_id) VALUES (?, ?)", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, name);
        statement.setInt(2, facultyId == null ? null : facultyId);

        int count = statement.executeUpdate();
        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            int id = rs.getInt(1);
            System.out.println("Добавлено отделение с идентификатором " + id);
        }
        System.out.println("Добавлено " + count + " отделений");
    }
    private static void addGroup(Connection connection, String name, Integer departmentId, Integer auditoriumId, Integer teacherId, Integer studentId) throws SQLException {
        if (name == null || name.isBlank()) {
            System.out.println("Название группы не может быть пустым");
            return;
        }
        if (departmentId != null) {
            System.out.println("Отделение с идентификатором " + departmentId + " не существует");
            return;
        }
        if (auditoriumId != null) {
            System.out.println("Аудитория с идентификатором " + auditoriumId + " не существует");
            return;
        }
        if (teacherId != null) {
            System.out.println("Учитель с идентификатором " + teacherId + " не существует");
            return;
        }
        if (studentId != null) {
            System.out.println("Студент с идентификатором " + studentId + " не существует");
            return;
        }

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO groups(name, department_id, auditorium_id, teacher_id, student_id) VALUES (?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, name);
        statement.setInt(2, departmentId == null ? null : departmentId);
        statement.setInt(3, auditoriumId == null ? null : auditoriumId);
        statement.setInt(4, teacherId == null ? null : teacherId);
        statement.setInt(5, studentId == null ? null : studentId);

        int count = statement.executeUpdate();

        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            int id = rs.getInt(1);
            System.out.println("Добавлена группа с идентификатором " + id);
        }
        System.out.println("Добавлено " + count + " групп");
        getGroup(connection);
    }
    //добавление студента и преподавателя в групппу
    private static void addStudentToGroup(Connection connection, int groupId, int studentId) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(
                "UPDATE groups SET student_id = ? WHERE id = ?");
        statement.setInt(1, studentId);
        statement.setInt(2, groupId);

        int count = statement.executeUpdate();

        if (count > 0) {
            System.out.println("Студент с идентификатором " + studentId + " добавлен в группу с идентификатором " + groupId);
        } else {
            System.out.println("Группа с идентификатором " + groupId + " не найдена");
        }
    }
    private static void addTeacherToGroup(Connection connection, int groupId, int teacherId) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(
                "UPDATE groups SET teacher_id = ? WHERE id = ?");
        statement.setInt(1, teacherId);
        statement.setInt(2, groupId);

        int count = statement.executeUpdate();

        if (count > 0) {
            System.out.println("Преподаватель с идентификатором " + teacherId + " добавлен в группу с идентификатором " + groupId);
        } else {
            System.out.println("Группа с идентификатором " + groupId + " не найдена");
        }
    }
    // удаление студента и преподавателя из группы
    private static void removeStudentFromGroup(Connection connection, int groupId, int studentId) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM groups WHERE id = ? AND student_id = ?");
        statement.setInt(1, groupId);
        statement.setInt(2, studentId);
        int count = statement.executeUpdate();
        if (count > 0) {
            System.out.println("Студент с идентификатором " + studentId + " удален из группы с идентификатором " + groupId);
        } else {
            System.out.println("Студент с идентификатором " + studentId + " не найден в группе с идентификатором " + groupId);
        }
    }
    private static void removeTeacherFromGroup(Connection connection, int groupId, int teacherId) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM groups WHERE id = ? AND teacher_id = ?");
        statement.setInt(1, groupId);
        statement.setInt(2, teacherId);
        int count = statement.executeUpdate();
        if (count > 0) {
            System.out.println("Преподаватель с идентификатором " + teacherId + " удален из группы с идентификатором " + groupId);
        } else {
            System.out.println("Преподаватель с идентификатором " + teacherId + " не найден в группе с идентификатором " + groupId);
        }
    }
    //получить список преподавателей с данной группы на указанной должности
    private static void getTeachersByGroupAndPosition(Connection connection, int groupId, String positionName) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(
                "SELECT teachers.id, teachers.firstName, teachers.lastName, teachers.education, teachers.workExperience " +
                        "FROM teachers " +
                        "JOIN groups ON teachers.id = groups.teacher_id " +
                        "JOIN position ON teachers.position_id = position.id " +
                        "WHERE groups.id = ? AND position.name = ?");
        statement.setInt(1, groupId);
        statement.setString(2, positionName);

        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            int id = rs.getInt("id");
            String firstName = rs.getString("firstName");
            String lastName = rs.getString("lastName");
            String education = rs.getString("education");
            int workExperience = rs.getInt("workExperience");

            System.out.println("ID: " + id + ", First Name: " + firstName + ", Last Name: " + lastName + ", Education: " + education + ", Work Experience: " + workExperience);
        }
    }
    // Обновить поле должность на «старший» + старая должность
    private static void updateTeacherPosition(Connection connection, int teacherId) throws SQLException {
        // обновляем поле position_id у преподавателя на новую должность
        PreparedStatement updateStatement = connection.prepareStatement(
                "UPDATE teachers SET position_id = (" +
                        "   SELECT id FROM position WHERE name = 'Старший ' || (SELECT name FROM position WHERE id = teachers.position_id) LIMIT 1" +
                        ") WHERE id = ?");
        updateStatement.setInt(1, teacherId);
        int count = updateStatement.executeUpdate();
        if (count > 0) {
            System.out.println("Должность преподавателя с идентификатором " + teacherId + " обновлена");
        } else {
            System.out.println("Преподаватель с идентификатором " + teacherId + " не найден");
        }
    }
    //получить студентов, которые родились позже заданного года
    private static void getStudentsBornAfterYear(Connection connection, int year) throws SQLException {
        PreparedStatement selectStatement = connection.prepareStatement(
                "SELECT * FROM students WHERE EXTRACT(YEAR FROM birthDate) > ?");
        selectStatement.setInt(1, year);
        ResultSet rs = selectStatement.executeQuery();

        while (rs.next()) {
            int id = rs.getInt("id");
            String firstName = rs.getString("firstName");
            String lastName = rs.getString("lastName");
            Date birthDate = rs.getDate("birthDate");
            int specialityId = rs.getInt("speciality_id");

            System.out.println("ID: " + id + ", First Name: " + firstName + ", Last Name: " + lastName + ", Birth Date: " + birthDate + ", Speciality ID: " + specialityId);
        }
    }







    //геттеры аудитории, должности, специальности, факультетов
    static void getPosition(Connection connection) throws SQLException{
        int param0 = -1;
        String param1 = null;

        Statement statement = connection.createStatement();                 // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM position;");  // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные
            param0 = rs.getInt(1); // значение ячейки, можно также получить по порядковому номеру (начиная с 1)
            param1 = rs.getString(2);

            System.out.println(param0 + " | " + param1);
        }
    }
    static void getSpeciality(Connection connection) throws SQLException{
        int param0 = -1;
        String param1 = null;

        Statement statement = connection.createStatement();                 // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM speciality;");  // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные
            param0 = rs.getInt(1); // значение ячейки, можно также получить по порядковому номеру (начиная с 1)
            param1 = rs.getString(2);

            System.out.println(param0 + " | " + param1);
        }
    }
    static void getAuditoriums(Connection connection) throws SQLException{
        int param0 = -1;
        String param1 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM auditoriums;");

        while (rs.next()) {  // пока есть данные
            param0 = rs.getInt(1);
            param1 = rs.getString(2);

            System.out.println(param0 + " | " + param1);
        }
    }

    static void getFaculty(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM faculty");
        while (rs.next()) {
            int id = rs.getInt("id");
            String name = rs.getString("name");
            int specialityId = rs.getInt("speciality_id");
            int positionId = rs.getInt("position_id");
            System.out.println("ID: " + id + ", Name: " + name + ", Speciality ID: " + specialityId + ", Position ID: " + positionId);
        }
    }

    private static void getStudent(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM students");
        while (rs.next()) {
            int id = rs.getInt("id");
            String firstName = rs.getString("firstName");
            String lastName = rs.getString("lastName");
            Date birthDate = rs.getDate("birthDate");
            int specialityId = rs.getInt("speciality_id");
            System.out.println("ID: " + id + ", First Name: " + firstName + ", Last Name: " + lastName + ", Birth Date: " + birthDate + ", Speciality ID: " + specialityId);
        }
    }

    private static void getTeacher(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM teachers");
        while (rs.next()) {
            int id = rs.getInt("id");
            String firstName = rs.getString("firstName");
            String lastName = rs.getString("lastName");
            String education = rs.getString("education");
            int workExperience = rs.getInt("workExperience");
            int positionId = rs.getInt("position_id");

            System.out.println("ID: " + id + ", First Name: " + firstName + ", Last Name: " + lastName + ", Education: " + education + ", Work Experience: " + workExperience + ", Position ID: " + positionId);
        }
    }
    private static void getGroup(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM groups");

        while (rs.next()) {
            int id = rs.getInt("id");
            String name = rs.getString("name");
            int departmentId = rs.getInt("department_id");
            int auditoriumId = rs.getInt("auditorium_id");
            int teacherId = rs.getInt("teacher_id");
            int studentId = rs.getInt("student_id");

            System.out.println("ID: " + id + ", Name: " + name + ", Department ID: " + departmentId + ", Auditorium ID: " + auditoriumId + ", Teacher ID: " + teacherId + ", Student ID: " + studentId);
        }
    }

}


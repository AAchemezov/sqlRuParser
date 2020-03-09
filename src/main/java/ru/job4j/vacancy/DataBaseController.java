package ru.job4j.vacancy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.Date;
import java.util.Properties;

/**
 * Контроллер работы с базой данных.
 */
public class DataBaseController implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(DataBaseController.class.getName());

    // Ключи конфигурации
    static final String DRIVER = "jdbc.driver";
    static final String URL = "jdbc.url";
    static final String USERNAME = "jdbc.username";
    static final String PASSWORD = "jdbc.password";
    // Имена элементов sql
    private static final String VACANCY_TABLE_NAME = "vacancy";
    private static final String MAX_DATE = "max_date";

    // sql - запросы
    private static final String SQL_LAST_VACANCY =
            "SELECT MAX(dateAdd) as " + MAX_DATE
                    + " FROM " + VACANCY_TABLE_NAME;

    private static final String SQL_INSERT_VACANCY =
            "INSERT INTO " + VACANCY_TABLE_NAME + "(name, text, link, dateAdd) VALUES (?,?,?,?);";

    private static final String SQL_IS_SCHEMA_CORRECT =
            "SELECT COUNT(t.table_name) "
                    + "FROM information_schema.tables t"
                    + " WHERE t.table_schema='public' AND t.table_name = '" + VACANCY_TABLE_NAME + "'";

    private static final String SQL_CREATE_TABLE_VACANCY =
            "create table " + VACANCY_TABLE_NAME + " ("
                    + "id serial primary key not null,"
                    + "name VARCHAR(512) unique,"
                    + "text TEXT, "
                    + "link VARCHAR(512),"
                    + "dateAdd bigint"
                    + ");";

    private final Connection connection;

    /**
     * Конструктор.
     *
     * @param config конфиг подключения к БД
     */
    DataBaseController(Properties config) throws SQLException, ClassNotFoundException {
        Class.forName(config.getProperty(DRIVER));
        this.connection = DriverManager.getConnection(
                config.getProperty(URL),
                config.getProperty(USERNAME),
                config.getProperty(PASSWORD)
        );
        if (!isSchemaCorrect()) {
            createTable();
        }
    }

    /**
     * Проверяет наличие таблицы {@link #VACANCY_TABLE_NAME} в схеме.
     */
    private boolean isSchemaCorrect() throws SQLException {
        boolean result = false;
        try (PreparedStatement ps = this.connection.prepareStatement(SQL_IS_SCHEMA_CORRECT)) {
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                result = rs.getInt("count") == 1;
            }
            rs.close();
        }
        return result;
    }

    /**
     * Создает таблицу {@link #VACANCY_TABLE_NAME}.
     */
    private void createTable() {
        try (Statement st = connection.createStatement()) {
            LOG.info("Создаю новую таблицу в базе данных");
            st.executeUpdate(SQL_CREATE_TABLE_VACANCY);
            LOG.info("Таблица создана");
        } catch (SQLException e) {
            LOG.error(e.getMessage());
        }
    }

    /**
     * Добавляет вакансию в БД. (Уникальность "name" вакансии обределяется в БД).
     *
     * @param name - имя
     * @param text - описание
     * @param link - url-адрес
     * @param date - дата добавления (последнего обновления)
     */
    public void addVacancy(String name, String text, String link, Date date) {
        try (PreparedStatement ps = connection.prepareStatement(SQL_INSERT_VACANCY)) {
            ps.setString(1, name);
            ps.setString(2, text);
            ps.setString(3, link);
            ps.setLong(4, date.getTime());
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.error(e.getMessage());
        }
    }

    /**
     * Возвращает дату добавления (изменения) последней добавленной вакансии.
     */
    public long getLastData() {
        long result = 0;
        try (PreparedStatement ps = connection.prepareStatement(SQL_LAST_VACANCY)) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    result = rs.getLong(MAX_DATE);
                }
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }
        return result;
    }

    @Override
    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }
}

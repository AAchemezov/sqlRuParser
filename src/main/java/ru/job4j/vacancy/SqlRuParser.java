package ru.job4j.vacancy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;
import static ru.job4j.vacancy.DataBaseController.*;

/**
 * Парсер "Java" вакансий сайта sql.ru. (https://www.sql.ru/forum/job-offers/)
 * <p>
 * Что сделать:
 * 1. Реализовать модуль сборки анализа данных с sql.ru.
 * 2. Система должна использовать Jsoup для парсинга страниц.
 * 3. Система должна запускаться раз в день, нужно использовать библиотеку quartz[ cron exression - 0 0 12 * * ?].
 * 4. Система должна собирать данные только про вакансии "java". учесть что "JavaScript" и "Java Script" не подходят.
 * 5. Данные должны храниться в базе данных.
 * 6. Учесть дубликаты. Вакансии с одинаковым именем считаются дубликатами.
 * 7. Учитывать время последнего запуска. Если это первый запуск, то нужно собрать все объявления с начало года.
 * 8. В системе не должно быть вывода либо ввода информации. все настройки должны быть в файле "app.properties".
 * 9. Для вывода нужной информации использовать логгер log4j.
 * 10. Пример запуска приложения: java -jar SqlRuParser.jar(jar path) app.properties(file path)
 */
public class SqlRuParser {

    private static final Logger LOG = LogManager.getLogger(SqlRuParser.class.getName());

    private static final String CRON_TIME = "cron.time";
    private static final List<String> CONFIG_KEYS = List.of(DRIVER, URL, USERNAME, PASSWORD, CRON_TIME);

    /**
     * Загружает и Возвращает конфиг приложения.
     *
     * @param path путь к файлу конфигурации
     * @return конфиг приложения
     */
    private static Properties getConfig(String path) {
        try (InputStream in = new FileInputStream(new File(path))) {
            Properties config = new Properties();
            config.load(in);
            return config;
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
        return null;
    }

    /**
     * Проверяет наличие обязательных свойств конфигурации
     *
     * @param config конфиг
     * @return true - если все свойства заданы
     */
    private static boolean checkConfig(Properties config) {
        if (config == null) {
            return false;
        }
//      todo расширить проверку при необходимости
        for (String key : CONFIG_KEYS) {
            if (!config.containsKey(key)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Создаёт и запускает задачу парсинга вакансий с сайта.
     *
     * @param config конфиг
     */
    static private void runParseJob(Properties config) {
        try {
            SchedulerFactory schedulerFactory = new StdSchedulerFactory();
            Scheduler scheduler = schedulerFactory.getScheduler();
            scheduler.start();
            JobDetail job = newJob(ParserJob.class)
                    .withIdentity("parseJob", "group1")
                    .usingJobData(DRIVER, config.getProperty(DRIVER))
                    .usingJobData(URL, config.getProperty(URL))
                    .usingJobData(USERNAME, config.getProperty(USERNAME))
                    .usingJobData(PASSWORD, config.getProperty(PASSWORD))
                    .build();
            CronTrigger trigger = newTrigger()
                    .withIdentity("trigger1", "group1")
                    .withSchedule(CronScheduleBuilder.cronSchedule(config.getProperty(CRON_TIME)))
                    .build();
            scheduler.scheduleJob(job, trigger);
        } catch (SchedulerException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * Точка входа в приложение.
     */
    public static void main(String[] args) {
        if (args.length == 0) {
            LOG.error("Отсутсвует путь к конфигурационному файлу");
            return;
        }
        Properties config = getConfig(args[0]);
        if (config == null) {
            LOG.error("Не удалось загрузить конфигурационный файл");
            return;
        }
        if (!checkConfig(config)) {
            LOG.error("Некоректный конфигурационный файл");
            return;
        }
        runParseJob(config);
    }
}

package ru.job4j.vacancy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;


/**
 * Задача (Job) парсинга вакансий "java" на sql.ru
 */
public class ParserJob implements Job {

    private static final Logger LOG = LogManager.getLogger(ParserJob.class.getName());
    private static final String URL = "https://www.sql.ru/forum/job-offers/";
    private static final Pattern JAVA_PATTERN = Pattern.compile("java");
    private static final String JAVA_SCRIPT_TEMPLATE = "(javascript|java script|java-script)";
    private static final Map<String, String> MOUNTS;
    private static final SimpleDateFormat DATE_AND_TIME_FORMAT = new SimpleDateFormat("dd MM yy, HH:mm");
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("dd MM yy");
    private static final int MAX_NUMBER_OLD_VACANCIES = 5/*Сколько просмотреть старых вакансий до окончания поиска*/;
    private static final int SKIP_LINES_ON_EACH_PAGE = 1 /*Шапка таблицы*/ + 3 /*Информационные сообщения*/;

    static {
        MOUNTS = new HashMap<>();
        MOUNTS.put("янв", "01");
        MOUNTS.put("фев", "02");
        MOUNTS.put("мар", "03");
        MOUNTS.put("апр", "04");
        MOUNTS.put("май", "05");
        MOUNTS.put("июн", "06");
        MOUNTS.put("июл", "07");
        MOUNTS.put("авг", "08");
        MOUNTS.put("сен", "09");
        MOUNTS.put("окт", "10");
        MOUNTS.put("ноя", "11");
        MOUNTS.put("дек", "12");
    }

    private DataBaseController dBController;
    private Date lastParsingDate;
    private int numberOfAddedVacancy;


    @Override
    public void execute(JobExecutionContext context) {
        try {
            JobDataMap dataMap = context.getMergedJobDataMap();
            dBController = new DataBaseController(
                    new Properties() {
                        {
                            putAll(dataMap.getWrappedMap());
                        }
                    }
            );
            LOG.info("Парсинг вакансий \"java\" на sql.ru ...");
            lastParsingDate = obtainLastParsingDate();
            parse();

            LOG.info("Парсинг завершен");
            LOG.info("Новых вакансий: " + numberOfAddedVacancy);
        } catch (SQLException | ClassNotFoundException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * Возвращает число страниц форума вакансий
     *
     * @return число страниц.
     */
    private Integer getCountPages() {
        int result = 0;
        try {
            Document doc = Jsoup.connect(URL).get();
            Elements elements = doc.getElementsByClass("sort_options").get(1).getElementsByTag("a");
            String i = elements.last().text();
            result = Integer.parseInt(i);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
        return result;
    }

    /**
     * Парсинг.
     */
    private void parse() {
        try {
            int oldViewed = 0;
            for (
//Начинаем с первой страницы
                    int page = 1;
//Пока есть непросмотренные страницы или просмотренно установленное число старых вакансий
                    page < getCountPages() && oldViewed != MAX_NUMBER_OLD_VACANCIES;
                    page++) {
                Document document;
//Получаем данные страницы
                document = Jsoup.connect(URL + page).get();
                Element table = document.getElementsByClass("forumTable").first();
                Elements trs = table.getElementsByTag("tr");
//Пробегаем по таблице вакансий, пропустив в таблице установленное число элементов
                for (int i = SKIP_LINES_ON_EACH_PAGE; i < trs.size() && oldViewed != MAX_NUMBER_OLD_VACANCIES; i++) {
                    Element tr = trs.get(i);
                    String stringDate = tr.getElementsByTag("td").last().text();
//получаем дату вакансии
                    Date date = DATE_AND_TIME_FORMAT.parse(normalizeDate(stringDate));
//Если дата ближе последней добавленной вакансии (или начала текущего года)
                    if (isAfterLastDate(date)) {
                        Element a = tr.getElementsByTag("td").get(1).getElementsByTag("a").first();
                        String name = a.text();
                        String link = a.attr("href");
//Если имя вакансии соответствует условиям поиска
                        if (isRelevanceJavaTemplate(name)) {
                            Document vacancy;
//Получаем документ по ссылке вакансии
                            vacancy = Jsoup.connect(link).get();
                            String text = vacancy.getElementsByClass("msgBody").get(1).text();
// Добавляем вакансию в БД
                            dBController.addVacancy(name, text, link, date);
                            LOG.info(
                                    "Добавлена вакансия [дата изменения:" + DATE_AND_TIME_FORMAT.format(date) + "]: " + name);
                            numberOfAddedVacancy++;
                        }
                    } else {
//Увеличиваем число просмотренных старых вакансий
                        oldViewed++;
                    }
                }
            }
        } catch (IOException | ParseException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * Нормализация текстового представления даты.
     */
    private static String normalizeDate(String date) {
        String[] strings = date.split(" ");
        switch (strings[0]) {
            case "сегодня,":
                return date.replace("сегодня", LocalDate.now().format(DATE_FORMAT));
            case "вчера,":
                return date.replace("вчера", LocalDate.now().minusDays(1).format(DATE_FORMAT));
            default:
                return date.replace(strings[1], MOUNTS.get(strings[1]));
        }
    }

    /**
     * Проверка даты вакансии.
     *
     * @param date дата
     * @return true дата после последней даты парсинга, иначе - false
     */
    private boolean isAfterLastDate(Date date) throws ParseException {
        return this.lastParsingDate.before(date);
    }

    /**
     * Получение даты последнего объявления.
     *
     * @return дату последнего запуска из параметров или 1 января текущего года
     */
    private Date obtainLastParsingDate() {
        long date = dBController.getLastData();
        if (date == 0) {
            return new GregorianCalendar(
                    Calendar.getInstance().get(Calendar.YEAR),
                    Calendar.JANUARY, 1, 0, 0, 0)
                    .getTime();
        }
        return new Date(date);
    }

    /**
     * Проверка имени вакансии на соответствие поисковым параметрам.
     *
     * @param name заговолок вакансии
     */
    private static boolean isRelevanceJavaTemplate(String name) {
        return JAVA_PATTERN.matcher(name
                .toLowerCase()
                .replaceAll(JAVA_SCRIPT_TEMPLATE, " ")
        ).find();
    }
}

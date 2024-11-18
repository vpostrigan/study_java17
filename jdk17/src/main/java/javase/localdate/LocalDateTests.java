package javase.localdate;

import java.time.Clock;
import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.chrono.HijrahDate;
import java.time.chrono.JapaneseDate;
import java.time.chrono.MinguoDate;
import java.time.chrono.ThaiBuddhistDate;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.TemporalQueries;
import java.time.temporal.TemporalQuery;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Instant          2013-08-20T15:16:26.355Z
 * LocalDate  	    2013-08-20
 * LocalDateTime  	2013-08-20T08:16:26.937
 * ZonedDateTime   	2013-08-21T00:16:26.941+09:00[Asia/Tokyo]
 * LocalTime   	    08:16:26.943
 * MonthDay         --08-20
 * Year    	        2013
 * YearMonth 	  	2013-08
 * Month    	    AUGUST
 * OffsetDateTime   2013-08-20T08:16:26.954-07:00
 * OffsetTime       08:16:26.957-07:00
 * Duration    	    PT20H (20 hours)
 * Period    	    P10D (10 days)
 */
public class LocalDateTests {

    public static void main(String[] args) throws InterruptedException {
        {
            LocalDateTime time = LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
            System.out.println(time);

            time = LocalDateTime.now();
            System.out.println(time);
        }
        {
            System.out.println("\n[nLocalDate]:");
            LocalDate today = LocalDate.now();
            System.out.println(today);

            LocalDate payday = today.with(TemporalAdjusters.lastDayOfMonth()).minusDays(2);
            System.out.println(payday);
        }
        {
            LocalDate dateOfBirth = LocalDate.of(2012, Month.MAY, 14);
            System.out.println(dateOfBirth);

            LocalDate firstBirthday = dateOfBirth.plusYears(1);
            System.out.println(firstBirthday);
        }

        {// dayOfWeekAndMonth_Enums
            System.out.printf("MONDAY + 3 = %s%n", DayOfWeek.MONDAY.plus(3));

            System.out.printf("MONDAY + 3 = %s%n", DayOfWeek.MONDAY.plus(3)
                    .getDisplayName(TextStyle.FULL, Locale.getDefault()));
            System.out.printf("MONDAY + 3 = %s%n", DayOfWeek.MONDAY.plus(3)
                    .getDisplayName(TextStyle.NARROW, Locale.getDefault()));
            System.out.printf("MONDAY + 3 = %s%n", DayOfWeek.MONDAY.plus(3)
                    .getDisplayName(TextStyle.SHORT, Locale.getDefault()));

            // //

            System.out.printf("Month.FEBRUARY: maxLength: %d%n", Month.FEBRUARY.maxLength());

            System.out.println(Month.FEBRUARY.getDisplayName(TextStyle.FULL, Locale.getDefault()));
            System.out.println(Month.FEBRUARY.getDisplayName(TextStyle.NARROW, Locale.getDefault()));
            System.out.println(Month.FEBRUARY.getDisplayName(TextStyle.SHORT, Locale.getDefault()));
        }

        { // dateClasses
            LocalDate date = LocalDate.of(2021, Month.MARCH, 10);
            LocalDate nextWed = date.with(TemporalAdjusters.next(DayOfWeek.WEDNESDAY));
            System.out.printf("%s ; nextWed %s%n", date, nextWed); // 2021-03-10 ; nextWed 2021-03-17

            DayOfWeek dotw = LocalDate.of(2012, Month.JULY, 9).getDayOfWeek();
            System.out.printf("%s%n", dotw); // MONDAY

            // //

            YearMonth ym = YearMonth.now();
            System.out.printf("%s: %d%n", ym, ym.lengthOfMonth());

            // //

            MonthDay md = MonthDay.of(Month.FEBRUARY, 29);
            boolean validLeapYear = md.isValidYear(2010);
            System.out.println(md + ": " + validLeapYear);

            // //

            validLeapYear = Year.of(2012).isLeap();
            System.out.println(Year.of(2012) + ": " + validLeapYear);
        }

        { // localTime
            System.out.println("\n[LocalTime]:");
            LocalTime now = LocalTime.now();
            System.out.println("LocalTime: " + now);

            // //

            System.out.printf("LocalDateTime now: %s%n", LocalDateTime.now());
            System.out.printf("Apr 15, 1994 @ 11:30am: %s%n", LocalDateTime.of(1994, Month.APRIL, 15, 11, 30));
            System.out.printf("now (from Instant): %s%n", LocalDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()));
            System.out.printf("6 months from now: %s%n", LocalDateTime.now().plusMonths(6));
            System.out.printf("6 months ago: %s%n", LocalDateTime.now().minusMonths(6));
        }

        { // zoneIdAndZoneOffset
            Set<String> allZones = ZoneId.getAvailableZoneIds();
            LocalDateTime dt = LocalDateTime.now();

            List<String> zoneList = new ArrayList<>(allZones);
            Collections.sort(zoneList);

            for (String s : zoneList) {
                ZoneId zone = ZoneId.of(s);
                ZonedDateTime zdt = dt.atZone(zone);
                ZoneOffset offset = zdt.getOffset();
                int secondsOfHour = offset.getTotalSeconds() % (60 * 60);
                String out = String.format("%35s %10s%n", zone, offset);

                // Write only time zones that do not have a whole hour offset to standard out.
                if (secondsOfHour != 0) {
                    System.out.printf(out);
                }
            }
        }

        { // timezones
            System.out.println("\n[timezones]:");

            DateTimeFormatter format = DateTimeFormatter.ofPattern("MMM d yyyy  hh:mm a", Locale.ENGLISH);

            // Leaving from San Francisco on July 20, 2013, at 7:30 p.m.
            LocalDateTime leaving = LocalDateTime.of(2013, Month.JULY, 20, 19, 30);
            ZoneId leavingZone = ZoneId.of("America/Los_Angeles");
            ZonedDateTime departure = ZonedDateTime.of(leaving, leavingZone);

            try {
                String out1 = departure.format(format); // LEAVING:  Jul 20 2013  07:30 PM (America/Los_Angeles)
                System.out.printf("LEAVING:  %s (%s)%n", out1, leavingZone);
            } catch (DateTimeException exc) {
                System.out.printf("%s can't be formatted!%n", departure);
                throw exc;
            }

            // Flight is 10 hours and 50 minutes, or 650 minutes
            ZoneId arrivingZone = ZoneId.of("Asia/Tokyo");
            ZonedDateTime arrival = departure.withZoneSameInstant(arrivingZone).plusMinutes(650);

            try {
                String out2 = arrival.format(format);
                System.out.printf("ARRIVING: %s (%s)%n", out2, arrivingZone); // ARRIVING: Jul 21 2013  10:20 PM (Asia/Tokyo)
            } catch (DateTimeException exc) {
                System.out.printf("%s can't be formatted!%n", arrival);
                throw exc;
            }

            if (arrivingZone.getRules().isDaylightSavings(arrival.toInstant()))
                System.out.printf("  (%s daylight saving time will be in effect.)%n", arrivingZone);
            else
                System.out.printf("  (%s standard time will be in effect.)%n", arrivingZone);
        }

        { // offsetDateTime
            System.out.println("\n[offsetDateTime]:");

            // Find the last Thursday in July 2013.
            LocalDateTime localDate = LocalDateTime.of(2013, Month.JULY, 20, 19, 30, 59);
            ZoneOffset offset = ZoneOffset.of("-08:00");

            OffsetDateTime offsetDate = OffsetDateTime.of(localDate, offset);
            System.out.println("offsetDate: " + offsetDate);
            OffsetDateTime lastThursday = offsetDate.with(TemporalAdjusters.lastInMonth(DayOfWeek.THURSDAY));
            System.out.printf("The last Thursday in July 2013 is the %sth.%n", lastThursday.getDayOfMonth());
        }

        { // instant (time in nanoseconds)
            System.out.println("\n[instant]:");

            Instant now = Instant.now();
            System.out.println(now);

            Instant oneHourLater = Instant.now().plus(1, ChronoUnit.HOURS);
            System.out.println(oneHourLater);

            long secondsFromEpoch = Instant.ofEpochSecond(0L).until(Instant.now(), ChronoUnit.SECONDS);
            System.out.println("secondsFromEpoch: " + secondsFromEpoch);

            Instant timestamp = Instant.now();
            LocalDateTime ldt = LocalDateTime.ofInstant(timestamp, ZoneId.systemDefault());
            System.out.printf("%s %d %d at %d:%d%n", ldt.getMonth(), ldt.getDayOfMonth(),
                    ldt.getYear(), ldt.getHour(), ldt.getMinute());
        }

        { // Temporal Package
            System.out.println("\n[Temporal Package]:");

            boolean isSupported = LocalDate.now().isSupported(ChronoField.CLOCK_HOUR_OF_DAY);
            System.out.println("ChronoField.CLOCK_HOUR_OF_DAY: " + isSupported);

            isSupported = Instant.now().isSupported(ChronoUnit.DAYS);
            System.out.println("ChronoUnit.DAYS: " + isSupported);

            // Predefined Adjusters
            LocalDate date = LocalDate.of(2000, Month.OCTOBER, 15);
            System.out.printf("%s is on a %s%n", date, date.getDayOfWeek()); // SUNDAY
            System.out.printf("first day of Month: %s%n",
                    date.with(TemporalAdjusters.firstDayOfMonth())); // 2000-10-01
            System.out.printf("first Monday of Month: %s%n",
                    date.with(TemporalAdjusters.firstInMonth(DayOfWeek.MONDAY))); // 2000-10-02
            System.out.printf("last day of Month: %s%n",
                    date.with(TemporalAdjusters.lastDayOfMonth())); // 2000-10-31
            System.out.printf("first day of next Month: %s%n",
                    date.with(TemporalAdjusters.firstDayOfNextMonth())); // 2000-11-01
            System.out.printf("first day of next Year: %s%n",
                    date.with(TemporalAdjusters.firstDayOfNextYear())); // 2001-01-01
            System.out.printf("first day of Year: %s%n",
                    date.with(TemporalAdjusters.firstDayOfYear())); // 2000-01-01

            // Custom Adjusters
            date = LocalDate.of(2013, Month.JUNE, 3);
            System.out.println("Given the date: " + date + " the next payday: " +
                    date.with(new PaydayAdjuster())); // 2013-06-14
            date = LocalDate.of(2013, Month.JUNE, 18);
            System.out.println("Given the date: " + date + " the next payday: " +
                    date.with(new PaydayAdjuster())); // 2013-06-28
            System.out.println();

            // Predefined Queries
            // The precision query returns the smallest ChronoUnit
            TemporalQuery<TemporalUnit> query = TemporalQueries.precision();
            System.out.printf("LocalDate precision is %s%n", LocalDate.now().query(query));
            System.out.printf("LocalDateTime precision is %s%n", LocalDateTime.now().query(query));
            System.out.printf("Year precision is %s%n", Year.now().query(query));
            System.out.printf("YearMonth precision is %s%n", YearMonth.now().query(query));
            System.out.printf("Instant precision is %s%n", Instant.now().query(query));

            // Custom Queries
            class FamilyVacations implements TemporalQuery<Boolean> {
                // Returns true if the passed-in date occurs during one of the
                // family vacations. Because the query compares the month and day only,
                // the check succeeds even if the Temporal types are not the same.
                public Boolean queryFrom(TemporalAccessor date) {
                    int month = date.get(ChronoField.MONTH_OF_YEAR);
                    int day = date.get(ChronoField.DAY_OF_MONTH);

                    // Disneyland over Spring Break
                    if ((month == Month.APRIL.getValue()) && ((day >= 3) && (day <= 8)))
                        return Boolean.TRUE;

                    // Smith family reunion on Lake Saugatuck
                    if ((month == Month.AUGUST.getValue()) && ((day >= 8) && (day <= 14)))
                        return Boolean.TRUE;

                    return Boolean.FALSE;
                }
            }
            class FamilyBirthdays {
                // Returns true if the passed-in date is the same as one of the
                // family birthdays. Because the query compares the month and day only,
                // the check succeeds even if the Temporal types are not the same.
                public Boolean isFamilyBirthday(TemporalAccessor date) {
                    int month = date.get(ChronoField.MONTH_OF_YEAR);
                    int day = date.get(ChronoField.DAY_OF_MONTH);

                    // Angie's birthday is on April 3.
                    if ((month == Month.APRIL.getValue()) && (day == 3))
                        return Boolean.TRUE;

                    // Sue's birthday is on June 18.
                    if ((month == Month.JUNE.getValue()) && (day == 18))
                        return Boolean.TRUE;

                    // Joe's birthday is on May 29.
                    if ((month == Month.MAY.getValue()) && (day == 29))
                        return Boolean.TRUE;

                    return Boolean.FALSE;
                }
            }

            // Invoking the query without using a lambda expression.
            Boolean isFamilyVacation = date.query(new FamilyVacations());

            // Invoking the query using a lambda expression.
            Boolean isFamilyBirthday = date.query(new FamilyBirthdays()::isFamilyBirthday);

            if (isFamilyVacation.booleanValue() || isFamilyBirthday.booleanValue())
                System.out.printf("%s is an important date!%n", date);
            else
                System.out.printf("%s is not an important date.%n", date);
        }
        { // Period and Duration
            System.out.println("\n[Duration]:");

            //  the duration between two instants
            Instant t1 = Instant.now();
            Thread.sleep(10);
            Instant t2 = Instant.now();
            long ns = Duration.between(t1, t2).toNanos();
            System.out.println("Duration.between: " + ns + "ns");

            // adds 10 seconds to an Instant
            Instant start = Instant.now();
            System.out.println("before 'adds 10 seconds': " + start);
            Duration gap = Duration.ofSeconds(10);
            Instant later = start.plus(gap);
            System.out.println("adds 10 seconds: " + later);

            System.out.println("\n[ChronoUnit]:");
            long gap2 = ChronoUnit.MILLIS.between(Instant.now().minus(Duration.ofSeconds(10)), Instant.now());
            System.out.println("\ngap2: " + gap2 + "ms");

            System.out.println("\n[Period]:");
            // how old you are, assuming that you were born on January 1, 1960.
            LocalDate today = LocalDate.now();
            LocalDate birthday = LocalDate.of(1960, Month.JANUARY, 1);

            Period p = Period.between(birthday, today);
            long p2 = ChronoUnit.DAYS.between(birthday, today);
            System.out.println("You are" +
                    " " + p.getYears() + " years," +
                    " " + p.getMonths() + " months, and" +
                    " " + p.getDays() + " days old." +
                    " (" + p2 + " days total)");

            // how long it is until your next birthday
            LocalDate nextBDay = birthday.withYear(today.getYear());
            // If your birthday has occurred this year already, add 1 to the year.
            if (nextBDay.isBefore(today) || nextBDay.isEqual(today)) {
                nextBDay = nextBDay.plusYears(1);
            }
            p = Period.between(today, nextBDay);
            p2 = ChronoUnit.DAYS.between(today, nextBDay);
            System.out.println("There are " +
                    p.getMonths() + " months, and " +
                    p.getDays() + " days until your next birthday. (" + p2 + " total)");
        }
        { // Period and Duration
            System.out.println("\n[Clock]:");
            System.out.println(Clock.systemUTC()); // SystemClock[Z]
            System.out.println("instant: " + Clock.systemUTC().instant()); // instant: 2021-11-23T18:59:49.716Z
            System.out.println("zone: " + Clock.systemUTC().getZone()); // zone: Z
            System.out.println("millis: " + Clock.systemUTC().millis()); // millis: 1637693989716
        }
        { // Non-ISO Date Conversion
            System.out.println("\n[Non-ISO Date Conversion]:");
            LocalDateTime date = LocalDateTime.of(2013, Month.JULY, 20, 19, 30);

            JapaneseDate jdate = JapaneseDate.from(date);
            HijrahDate hdate = HijrahDate.from(date);
            MinguoDate mdate = MinguoDate.from(date);
            ThaiBuddhistDate tdate = ThaiBuddhistDate.from(date);
            System.out.println("JapaneseDate: " + jdate);
            System.out.println("HijrahDate: " + hdate);
            System.out.println("MinguoDate: " + mdate);
            System.out.println("ThaiBuddhistDate: " + tdate);

            // Converting to an ISO-Based Date
            System.out.println("\n[Converting to an ISO-Based Date]:");
            LocalDate date2 = LocalDate.from(JapaneseDate.now());
            System.out.println(date2); // will show today's date 2021-11-24
        }
        { // Legacy Date-Time Code
            System.out.println("\n[Legacy Date-Time Code]:");
            Calendar now = Calendar.getInstance();
            ZonedDateTime zdt = ZonedDateTime.ofInstant(now.toInstant(), ZoneId.systemDefault());
            System.out.println("ZonedDateTime: " + zdt); // ZonedDateTime: 2021-11-24T15:11:33.053+02:00[Europe/Helsinki]

            Instant inst = new Date().toInstant();
            Date newDate = Date.from(inst);
            System.out.println("Instant: " + inst); // Instant: 2021-11-24T13:11:33.056Z
            System.out.println("Date from Instant: " + newDate); // Date from Instant: Wed Nov 24 15:11:33 EET 2021
        }

        { // Exercises
            System.out.println("\n[for a given year, reports the length of each month within that year]:");
            Year year = Year.of(2021);
            System.out.printf("For the year %s:%n", year);
            for (Month month : Month.values()) {
                YearMonth ym = YearMonth.of(year.getValue(), month);
                System.out.printf("%s: %d days%n", month, ym.lengthOfMonth());
            }

            System.out.println("\n[for a given month of the current year, lists all of the Mondays in that month]:");
            Month month = Month.valueOf("March".toUpperCase());
            System.out.printf("For the month of %s:%n", month);
            LocalDate date = Year.now().atMonth(month).atDay(1).
                    with(TemporalAdjusters.firstInMonth(DayOfWeek.MONDAY));
            while (date.getMonth() == month) {
                System.out.printf("%s%n", date);
                date = date.with(TemporalAdjusters.next(DayOfWeek.MONDAY));
            }

            System.out.println("\n[tests whether a given date occurs on Friday the 13th]:");
            month = Month.valueOf("August".toUpperCase());
            int day = 13;
            date = Year.now().atMonth(month).atDay(day);

            System.out.println(date.query(date0 ->
                    (date0.get(ChronoField.DAY_OF_MONTH) == 13) &&
                            (date0.get(ChronoField.DAY_OF_WEEK) == 5)
            ).toString());
        }
    }

    /**
     * This temporal adjuster assumes that payday occurs on the 15th
     * and the last day of each month. However, if either of those
     * days lands on a weekend, then the previous Friday is used.
     */
    private static class PaydayAdjuster implements TemporalAdjuster {

        /**
         * The adjustInto method accepts a Temporal instance
         * and returns an adjusted LocalDate. If the passed in
         * parameter is not a LocalDate, then a DateTimeException is thrown.
         */
        public Temporal adjustInto(Temporal input) {
            LocalDate date = LocalDate.from(input);
            int day;
            if (date.getDayOfMonth() < 15) {
                day = 15;
            } else {
                day = date.with(TemporalAdjusters.lastDayOfMonth()).getDayOfMonth();
            }
            date = date.withDayOfMonth(day);
            if (date.getDayOfWeek() == DayOfWeek.SATURDAY ||
                    date.getDayOfWeek() == DayOfWeek.SUNDAY) {
                date = date.with(TemporalAdjusters.previous(DayOfWeek.FRIDAY));
            }

            return input.with(date);
        }
    }

}

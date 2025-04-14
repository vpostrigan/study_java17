package ch07.fig07_14;

// Fig. 7.15: GradeBookTest.java
// GradeBookTest creates a GradeBook object using an array of grades, 
// then invokes method processGrades to analyze them.
public class GradeBookTest {

    public static void main(String[] args) {
        // array of student grades
        int[] gradesArray = {87, 68, 94, 100, 83, 78, 85, 91, 76, 87};

        GradeBook myGradeBook = new GradeBook("CS101 Introduction to Java Programming", gradesArray);
        System.out.printf("Welcome to the grade book for%n%s%n%n", myGradeBook.getCourseName());
        myGradeBook.processGrades();
    }
/*
Welcome to the grade book for
CS101 Introduction to Java Programming

The grades are:

Student  1:  87
Student  2:  68
Student  3:  94
Student  4: 100
Student  5:  83
Student  6:  78
Student  7:  85
Student  8:  91
Student  9:  76
Student 10:  87

Class average is 84.90
Lowest grade is 68
Highest grade is 100

Grade distribution:
00-09:
10-19:
20-29:
30-39:
40-49:
50-59:
60-69: *
70-79: **
80-89: ****
90-99: **
  100: *
 */
}

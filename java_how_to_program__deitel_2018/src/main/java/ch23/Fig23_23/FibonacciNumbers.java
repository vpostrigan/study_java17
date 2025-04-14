package ch23.Fig23_23;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

// FibonacciNumbers.java
// Main application class that loads and displays the FibonacciNumbers GUI.
public class FibonacciNumbers extends Application {
    @Override
    public void start(Stage stage) throws Exception {
        Parent root = FXMLLoader.load(getClass().getResource("FibonacciNumbers.fxml"));

        Scene scene = new Scene(root);
        stage.setTitle("Fibonacci Numbers");
        stage.setScene(scene);
        stage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }
}

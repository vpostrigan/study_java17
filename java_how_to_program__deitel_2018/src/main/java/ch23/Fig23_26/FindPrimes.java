package ch23.Fig23_26;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

// FindPrimes.java
// Main application class that loads and displays the FindPrimes GUI.
public class FindPrimes extends Application {
    @Override
    public void start(Stage stage) throws Exception {
        Parent root = FXMLLoader.load(getClass().getResource("FindPrimes.fxml"));

        Scene scene = new Scene(root);
        stage.setTitle("Find Primes");
        stage.setScene(scene);
        stage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }
}

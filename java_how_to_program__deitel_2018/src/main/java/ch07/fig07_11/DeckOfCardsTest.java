package ch07.fig07_11;

// Fig. 7.13: DeckOfCardsTest.java
// Card shuffling and dealing.

public class DeckOfCardsTest {

    // execute application
    public static void main(String[] args) {
        DeckOfCards myDeckOfCards = new DeckOfCards();
        myDeckOfCards.shuffle(); // place Cards in random order

        // print all 52 Cards in the order in which they are dealt
        for (int i = 1; i <= 52; i++) {
            // deal and display a Card
            System.out.printf("%-19s", myDeckOfCards.dealCard());

            if (i % 4 == 0) { // output a newline after every fourth card
                System.out.println();
            }
        }
    }
/*
Eight of Spades    Ten of Diamonds    Ace of Clubs       Deuce of Diamonds
Four of Clubs      Seven of Hearts    King of Hearts     Deuce of Spades
Four of Spades     Seven of Clubs     Jack of Diamonds   Five of Spades
Four of Hearts     Jack of Clubs      Jack of Spades     Five of Hearts
Queen of Clubs     Deuce of Clubs     Six of Clubs       Ten of Hearts
Jack of Hearts     Queen of Diamonds  Seven of Diamonds  Nine of Diamonds
Three of Diamonds  Ace of Spades      Ace of Diamonds    King of Spades
Six of Diamonds    Ten of Spades      Four of Diamonds   Three of Hearts
Eight of Diamonds  Seven of Spades    Queen of Hearts    Five of Clubs
Nine of Spades     Six of Hearts      King of Diamonds   Deuce of Hearts
King of Clubs      Six of Spades      Ten of Clubs       Nine of Clubs
Queen of Spades    Three of Spades    Nine of Hearts     Five of Diamonds
Eight of Clubs     Three of Clubs     Ace of Hearts      Eight of Hearts
 */
}

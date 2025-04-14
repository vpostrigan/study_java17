package ch16;

import java.util.List;
import java.util.Arrays;
import java.util.Collections;

// Fig. 16.10: DeckOfCards.java
// Card shuffling and dealing with Collections method shuffle.

// class to represent a Card in a deck of cards
class Card {
    public enum Face {
        Ace, Deuce, Three, Four, Five, Six,
        Seven, Eight, Nine, Ten, Jack, Queen, King
    }

    public enum Suit {Clubs, Diamonds, Hearts, Spades}

    private final Face face;
    private final Suit suit;

    // constructor
    public Card(Face face, Suit suit) {
        this.face = face;
        this.suit = suit;
    }

    // return face of the card
    public Face getFace() {
        return face;
    }

    // return suit of Card
    public Suit getSuit() {
        return suit;
    }

    // return String representation of Card
    @Override
    public String toString() {
        return String.format("%s of %s", face, suit);
    }
}

// class DeckOfCards declaration
public class DeckOfCards {
    private List<Card> list; // declare List that will store Cards

    // set up deck of Cards and shuffle
    public DeckOfCards() {
        Card[] deck = new Card[52];
        int count = 0; // number of cards

        // populate deck with Card objects
        for (Card.Suit suit : Card.Suit.values()) {
            for (Card.Face face : Card.Face.values()) {
                deck[count] = new Card(face, suit);
                ++count;
            }
        }

        list = Arrays.asList(deck); // get List
        Collections.shuffle(list);  // shuffle deck
    }

    // output deck
    public void printCards() {
        // display 52 cards in four columns
        for (int i = 0; i < list.size(); i++) {
            System.out.printf("%-19s%s", list.get(i), ((i + 1) % 4 == 0) ? System.lineSeparator() : "");
        }
    }

    public static void main(String[] args) {
        DeckOfCards cards = new DeckOfCards();
        cards.printCards();
    }
}
/*
Jack of Clubs      Eight of Hearts    Three of Hearts    Ten of Clubs
Nine of Clubs      Eight of Spades    Six of Spades      King of Clubs
Ace of Spades      Seven of Hearts    Four of Hearts     Eight of Clubs
Deuce of Diamonds  King of Hearts     Seven of Clubs     Five of Diamonds
Ace of Hearts      Ace of Diamonds    Deuce of Spades    Nine of Diamonds
Four of Diamonds   Queen of Hearts    Six of Diamonds    Five of Clubs
Queen of Clubs     Three of Spades    Jack of Spades     Five of Spades
Three of Clubs     Deuce of Hearts    Nine of Spades     Ten of Diamonds
Jack of Hearts     Nine of Hearts     Four of Clubs      Ten of Hearts
King of Spades     Queen of Diamonds  Seven of Diamonds  Jack of Diamonds
Six of Hearts      Three of Diamonds  Four of Spades     Ten of Spades
Seven of Spades    Eight of Diamonds  Ace of Clubs       Queen of Spades
Five of Hearts     King of Diamonds   Six of Clubs       Deuce of Clubs
 */

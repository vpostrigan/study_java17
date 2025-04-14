package ch21;

// Fig. 21.15: Tree.java
// TreeNode and Tree class declarations for a binary search tree.

import java.security.SecureRandom;

// class Tree definition
public class Tree<E extends Comparable<E>> {
    private TreeNode<E> root;

    // constructor initializes an empty Tree of integers
    public Tree() {
        root = null;
    }

    // insert a new node in the binary search tree
    public void insertNode(E insertValue) {
        if (root == null) {
            root = new TreeNode<>(insertValue); // create root node
        } else {
            root.insert(insertValue); // call the insert method
        }
    }

    // begin preorder traversal
    public void preorderTraversal() {
        preorderHelper(root);
    }

    // recursive method to perform preorder traversal
    private void preorderHelper(TreeNode<E> node) {
        if (node == null) {
            return;
        }

        System.out.printf("%s ", node.data); // output node data
        preorderHelper(node.leftNode); // traverse left subtree
        preorderHelper(node.rightNode); // traverse right subtree
    }

    // begin inorder traversal
    public void inorderTraversal() {
        inorderHelper(root);
    }

    // recursive method to perform inorder traversal
    private void inorderHelper(TreeNode<E> node) {
        if (node == null) {
            return;
        }

        inorderHelper(node.leftNode); // traverse left subtree
        System.out.printf("%s ", node.data); // output node data
        inorderHelper(node.rightNode); // traverse right subtree
    }

    // begin postorder traversal
    public void postorderTraversal() {
        postorderHelper(root);
    }

    // recursive method to perform postorder traversal
    private void postorderHelper(TreeNode<E> node) {
        if (node == null) {
            return;
        }

        postorderHelper(node.leftNode); // traverse left subtree
        postorderHelper(node.rightNode); // traverse right subtree
        System.out.printf("%s ", node.data); // output node data
    }

}


// class TreeNode definition
class TreeNode<E extends Comparable<E>> {
    // package access members
    TreeNode<E> leftNode;
    E data; // node value
    TreeNode<E> rightNode;

    // constructor initializes data and makes this a leaf node
    public TreeNode(E nodeData) {
        data = nodeData;
        leftNode = rightNode = null; // node has no children
    }

    // locate insertion point and insert new node; ignore duplicate values
    public void insert(E insertValue) {
        // insert in left subtree
        if (insertValue.compareTo(data) < 0) {
            // insert new TreeNode
            if (leftNode == null) {
                leftNode = new TreeNode<>(insertValue);
            } else { // continue traversing left subtree recursively
                leftNode.insert(insertValue);
            }
        }
        // insert in right subtree
        else if (insertValue.compareTo(data) > 0) {
            // insert new TreeNode
            if (rightNode == null) {
                rightNode = new TreeNode<>(insertValue);
            } else { // continue traversing right subtree recursively
                rightNode.insert(insertValue);
            }
        }
    }
}

// Fig. 21.16: TreeTest.java
// Binary tree test program.
class TreeTest {

    public static void main(String[] args) {
        Tree<Integer> tree = new Tree<>();
        SecureRandom randomNumber = new SecureRandom();

        System.out.println("Inserting the following values: ");

        // insert 10 random integers from 0-99 in tree
        for (int i = 1; i <= 10; i++) {
            int value = randomNumber.nextInt(100);
            System.out.printf("%d ", value);
            tree.insertNode(value);
        }

        System.out.printf("%n%nPreorder traversal%n");
        tree.preorderTraversal();

        System.out.printf("%n%nInorder traversal%n");
        tree.inorderTraversal();

        System.out.printf("%n%nPostorder traversal%n");
        tree.postorderTraversal();
        System.out.println();
    }
/*
Inserting the following values:
81 57 46 9 17 83 40 49 95 44

Preorder traversal
81 57 46 9 17 40 44 49 83 95

Inorder traversal
9 17 40 44 46 49 57 81 83 95

Postorder traversal
44 40 17 9 49 46 57 95 83 81
 */
}

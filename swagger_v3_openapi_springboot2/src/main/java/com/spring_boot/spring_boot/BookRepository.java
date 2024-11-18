package com.spring_boot.spring_boot;

import com.spring_boot.domain.Book;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Collection;

@Repository
public interface BookRepository extends JpaRepository<Book, Long> {
    Book findByName(String productName);

    Collection<Book> getBooks();

    Page<Book> getBooks(Pageable pageable);
}
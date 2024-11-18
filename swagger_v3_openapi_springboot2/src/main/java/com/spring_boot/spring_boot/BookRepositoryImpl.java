package com.spring_boot.spring_boot;

import com.spring_boot.domain.Book;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.util.Collection;

@Service
@Transactional
public class BookRepositoryImpl {

    @Lazy
    @Autowired
    private BookRepository bookRepository;

    public Collection<Book> getBooks() {
        PageRequest request = PageRequest.of(0, 1000);
        return bookRepository.findAll(request).getContent();
    }

    public Page<Book> getBooks(Pageable pageable) {
        PageRequest request = PageRequest.of(0, 1000);
        return bookRepository.findAll(request);
    }

}

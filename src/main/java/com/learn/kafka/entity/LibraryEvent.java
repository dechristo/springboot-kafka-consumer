package com.learn.kafka.entity;

import lombok.*;

import javax.persistence.*;


@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class LibraryEvent {
    @Id
    @GeneratedValue
    private Integer eventId;
    @Enumerated(EnumType.STRING)
    private LibraryEventType eventType;
    @OneToOne(mappedBy = "libraryEvent")
    @ToString.Exclude
    private Book book;
}


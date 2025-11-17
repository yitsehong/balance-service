package io.xrex.persistence.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Getter
@Setter
@Entity
@Table(name = "account")
public class AccountEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private Integer uid;
    private Integer type;
    private BigDecimal balance;
    private String tag;
    private LocalDateTime ctime;
    private LocalDateTime mtime;
}

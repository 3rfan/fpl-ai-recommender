package com.erfan.fpl_ai_recommender.domain;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Represents player statistics for a specific gameweek match.
 */
@Entity
@Table(name = "player_match_stats", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"player_id", "gameweek"})
})
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Builder
public class PlayerMatchStats {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "stats_id")
    private Long statsId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "player_id", nullable = false)
    private Player player;

    @Column(nullable = false)
    private Integer gameweek;

    @Column(nullable = false)
    private Integer minutes;

    @Column(nullable = false)
    private Integer goals;

    @Column(nullable = false)
    private Integer assists;

    @Column(name = "xg", precision = 5, scale = 2)
    private BigDecimal xg;

    @Column(name = "xa", precision = 5, scale = 2)
    private BigDecimal xa;

    @Column(nullable = false)
    private Integer shots;

    @Column(name = "key_passes", nullable = false)
    private Integer keyPasses;

    @Column(name = "clean_sheet", nullable = false)
    private Boolean cleanSheet;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @Override
    public String toString() {
        return "PlayerMatchStats{" +
                "statsId=" + statsId +
                ", gameweek=" + gameweek +
                ", minutes=" + minutes +
                ", goals=" + goals +
                ", assists=" + assists +
                '}';
    }
}


package com.erfan.fpl_ai_recommender.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

/**
 * Represents a fixture/match between two teams in a specific gameweek.
 */
@Entity
@Table(name = "fixtures")
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Builder
public class Fixture {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "fixture_id")
    private Long fixtureId;

    @Column(nullable = false)
    private Integer gameweek;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "home_team_id", nullable = false)
    private Team homeTeam;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "away_team_id", nullable = false)
    private Team awayTeam;

    @Column(name = "match_date")
    private LocalDateTime matchDate;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @Override
    public String toString() {
        return "Fixture{" +
                "fixtureId=" + fixtureId +
                ", gameweek=" + gameweek +
                ", matchDate=" + matchDate +
                '}';
    }
}


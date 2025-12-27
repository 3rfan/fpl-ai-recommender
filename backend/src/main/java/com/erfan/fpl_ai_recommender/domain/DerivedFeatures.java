package com.erfan.fpl_ai_recommender.domain;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Represents derived features for a player in a specific gameweek.
 */
@Entity
@Table(name = "derived_features", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"player_id", "gameweek"})
})
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Builder
public class DerivedFeatures {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "feature_id")
    private Long featureId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "player_id", nullable = false)
    private Player player;

    @Column(nullable = false)
    private Integer gameweek;

    @Column(name = "xg_per_90", precision = 5, scale = 2)
    private BigDecimal xgPer90;

    @Column(name = "xa_per_90", precision = 5, scale = 2)
    private BigDecimal xaPer90;

    @Column(name = "expected_minutes", precision = 5, scale = 2)
    private BigDecimal expectedMinutes;

    @Column(name = "opponent_difficulty", precision = 4, scale = 2)
    private BigDecimal opponentDifficulty;

    @Column(name = "home_away", length = 10)
    private String homeAway;

    @Column(name = "clean_sheet_probability", precision = 4, scale = 3)
    private BigDecimal cleanSheetProbability;

    @Column(name = "risk_flag", length = 20)
    private String riskFlag;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @Override
    public String toString() {
        return "DerivedFeatures{" +
                "featureId=" + featureId +
                ", gameweek=" + gameweek +
                ", xgPer90=" + xgPer90 +
                ", xaPer90=" + xaPer90 +
                ", expectedMinutes=" + expectedMinutes +
                '}';
    }
}


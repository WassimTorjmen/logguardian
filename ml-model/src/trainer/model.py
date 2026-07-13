"""
LSTM Autoencoder pour la détection d'anomalies dans les logs.

Principe : entraîné uniquement sur des logs normaux.
L'erreur de reconstruction (MSE) est faible pour les séquences normales
et élevée pour les séquences anormales.

Architecture (from scratch) :
- Encodeur LSTM bidirectionnel (capture le contexte passé ET futur)
- Dropout pour éviter le surapprentissage
- Mécanisme d'attention dans le décodeur (pondère les pas de temps importants)
- 2 couches LSTM empilées pour plus de profondeur

SCHÉMA COMPLET :
Input (10 logs × 77 dims)
    → LSTM bidirectionnel → h_fwd(64) + h_bwd(64) → concat(128) → Linear → 32 (latent)
    → Linear(32→64) → répété 10 fois → LSTM décodeur → Attention → Linear → 77 (reconstruction)
    → MSE(input, reconstruction) = score d'anomalie
"""
import torch
import torch.nn as nn
import torch.nn.functional as F


class _Attention(nn.Module):
    """
    Mécanisme d'attention : donne un score d'importance à chaque pas de temps.
    Les pas de temps les plus importants auront plus de poids dans la reconstruction.
    """

    def __init__(self, hidden_size: int):
        super().__init__()
        # Couche linéaire qui prend 64 dims et retourne 1 seul score d'importance
        self.attn = nn.Linear(hidden_size, 1)

    def forward(self, decoder_out: torch.Tensor) -> torch.Tensor:
        # decoder_out : (batch, seq_len, hidden_size) = (64, 10, 64)

        # Calcule un score d'importance pour chaque pas de temps
        scores = self.attn(decoder_out)         # (batch, seq_len, 1)

        # Transforme les scores en pourcentages qui somment à 1
        # Exemple : [2.1, 0.3, 1.8, ...] → [0.55, 0.08, 0.37, ...]
        weights = F.softmax(scores, dim=1)      # (batch, seq_len, 1)

        # Multiplie chaque pas de temps par son importance
        return decoder_out * weights            # (batch, seq_len, hidden_size)


class LSTMAutoencoder(nn.Module):
    def __init__(
        self,
        n_features: int,    # 77 — taille d'un vecteur log
        hidden_size: int,   # 64 — taille de la mémoire interne du LSTM
        latent_size: int,   # 32 — taille du vecteur compressé (résumé)
        seq_len: int,       # 10 — nombre de logs par séquence
        num_layers: int = 2,   # 2 couches LSTM empilées
        dropout: float = 0.2,  # 20% des neurones désactivés pendant l'entraînement
    ):
        super().__init__()
        self.seq_len = seq_len
        self.hidden_size = hidden_size
        self.num_layers = num_layers

        # ── ENCODEUR ──────────────────────────────────────────────────────────
        # LSTM bidirectionnel : lit la séquence dans les 2 sens (gauche→droite ET droite→gauche)
        # Produit 2 × hidden_size = 128 dimensions (une par direction)
        self.encoder_lstm = nn.LSTM(
            input_size=n_features,      # reçoit 77 dims
            hidden_size=hidden_size,    # produit 64 dims par direction
            num_layers=num_layers,      # 2 couches empilées
            batch_first=True,
            bidirectional=True,         # les 2 sens simultanément
            dropout=dropout if num_layers > 1 else 0.0,
        )
        # Désactive 20% des neurones aléatoirement pour éviter le surapprentissage
        self.encoder_dropout = nn.Dropout(dropout)
        # Compresse 128 (forward + backward) → 32 (vecteur latent)
        self.encoder_fc = nn.Linear(hidden_size * 2, latent_size)

        # ── DÉCODEUR ──────────────────────────────────────────────────────────
        # Expand le vecteur latent 32 → 64 pour le donner au LSTM décodeur
        self.decoder_fc = nn.Linear(latent_size, hidden_size)
        # LSTM décodeur : unidirectionnel (pas besoin des 2 sens pour reconstruire)
        self.decoder_lstm = nn.LSTM(
            input_size=hidden_size,     # reçoit 64 dims
            hidden_size=hidden_size,    # produit 64 dims
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        # Mécanisme d'attention pour pondérer les pas de temps importants
        self.attention = _Attention(hidden_size)
        self.decoder_dropout = nn.Dropout(dropout)
        # Revient à 77 dimensions = reconstruction du log original
        self.output_fc = nn.Linear(hidden_size, n_features)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x : (batch, seq_len, n_features) = (64, 10, 77)
        # Fonction appelée automatiquement quand on donne des données au modèle

        # ── ENCODAGE ──────────────────────────────────────────────────────────
        # On passe la séquence dans le LSTM bidirectionnel
        # On récupère uniquement h_n = état caché final = résumé de toute la séquence
        _, (h_n, _) = self.encoder_lstm(x)

        # h_n contient les états finaux des 2 directions, on les sépare
        h_fwd = h_n[-2]                                     # direction gauche → droite (64 dims)
        h_bwd = h_n[-1]                                     # direction droite → gauche (64 dims)

        # On colle les 2 directions bout à bout → 128 dimensions
        h_cat = torch.cat([h_fwd, h_bwd], dim=-1)          # (batch, 128)
        h_cat = self.encoder_dropout(h_cat)

        # Compression 128 → 32 : le vecteur latent = résumé compressé de la séquence
        latent = self.encoder_fc(h_cat)                     # (batch, 32)

        # ── DÉCODAGE ──────────────────────────────────────────────────────────
        # Expand 32 → 64
        dec = self.decoder_fc(latent)                       # (batch, 64)

        # On répète le vecteur 10 fois pour avoir 10 pas de temps
        # Le LSTM introduira ensuite la variation temporelle
        dec = dec.unsqueeze(1).repeat(1, self.seq_len, 1)  # (batch, 10, 64)

        # LSTM décodeur traite les 10 répétitions
        dec_out, _ = self.decoder_lstm(dec)                 # (batch, 10, 64)

        # L'attention pondère les pas de temps importants
        dec_out = self.attention(dec_out)
        dec_out = self.decoder_dropout(dec_out)

        # Revient à 77 dimensions = reconstruction du log original
        out = self.output_fc(dec_out)                       # (batch, 10, 77)

        return out

    def reconstruction_error(self, x: torch.Tensor) -> torch.Tensor:
        """
        Calcule le score d'anomalie = MSE entre l'original et la reconstruction.

        Score faible  → séquence normale (bien reconstruite)
        Score élevé   → séquence anormale (mal reconstruite) → ALERTE si > seuil

        Calcul :
          x - out   = différence entre original et reconstruction
          ** 2      = on élève au carré (toujours positif, pénalise les grandes erreurs)
          .mean()   = moyenne → 1 seul nombre par séquence
        """
        with torch.no_grad():   # pas de calcul de gradients ici, c'est de l'évaluation
            out = self.forward(x)
            return ((x - out) ** 2).mean(dim=(1, 2))       # (batch,)

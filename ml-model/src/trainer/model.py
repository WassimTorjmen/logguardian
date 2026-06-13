"""
LSTM Autoencoder pour la détection d'anomalies dans les logs.

Principe : entraîné uniquement sur des logs normaux.
L'erreur de reconstruction (MSE) est faible pour les séquences normales
et élevée pour les séquences anormales.

Architecture améliorée :
- Encodeur LSTM bidirectionnel (capture le contexte passé ET futur)
- Dropout pour régulariser
- Mécanisme d'attention dans le décodeur (pondère les pas de temps importants)
- Couches multiples (num_layers=2 par défaut)
"""
import torch
import torch.nn as nn
import torch.nn.functional as F


class _Attention(nn.Module):
    """Attention scalaire sur les sorties du décodeur LSTM."""

    def __init__(self, hidden_size: int):
        super().__init__()
        self.attn = nn.Linear(hidden_size, 1)

    def forward(self, decoder_out: torch.Tensor) -> torch.Tensor:
        # decoder_out : (batch, seq_len, hidden_size)
        scores = self.attn(decoder_out)                      # (batch, seq_len, 1)
        weights = F.softmax(scores, dim=1)                   # (batch, seq_len, 1)
        return decoder_out * weights                         # (batch, seq_len, hidden_size)


class LSTMAutoencoder(nn.Module):
    def __init__(
        self,
        n_features: int,
        hidden_size: int,
        latent_size: int,
        seq_len: int,
        num_layers: int = 2,
        dropout: float = 0.2,
    ):
        super().__init__()
        self.seq_len = seq_len
        self.hidden_size = hidden_size
        self.num_layers = num_layers

        # Encodeur bidirectionnel — produit 2×hidden_size par couche
        self.encoder_lstm = nn.LSTM(
            input_size=n_features,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            bidirectional=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.encoder_dropout = nn.Dropout(dropout)
        # Les deux directions sont concaténées → 2*hidden_size → latent
        self.encoder_fc = nn.Linear(hidden_size * 2, latent_size)

        # Décodeur unidirectionnel avec attention
        self.decoder_fc = nn.Linear(latent_size, hidden_size)
        self.decoder_lstm = nn.LSTM(
            input_size=hidden_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.attention = _Attention(hidden_size)
        self.decoder_dropout = nn.Dropout(dropout)
        self.output_fc = nn.Linear(hidden_size, n_features)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x : (batch, seq_len, n_features)

        # Encodage bidirectionnel
        _, (h_n, _) = self.encoder_lstm(x)
        # h_n : (num_layers * 2, batch, hidden_size) — forward et backward entrelacés
        # On prend la dernière couche forward + backward
        h_fwd = h_n[-2]                                      # (batch, hidden_size)
        h_bwd = h_n[-1]                                      # (batch, hidden_size)
        h_cat = torch.cat([h_fwd, h_bwd], dim=-1)           # (batch, 2*hidden_size)
        h_cat = self.encoder_dropout(h_cat)
        latent = self.encoder_fc(h_cat)                      # (batch, latent_size)

        # Décodage avec attention
        dec = self.decoder_fc(latent)                        # (batch, hidden_size)
        dec = dec.unsqueeze(1).repeat(1, self.seq_len, 1)   # (batch, seq_len, hidden_size)
        dec_out, _ = self.decoder_lstm(dec)                  # (batch, seq_len, hidden_size)
        dec_out = self.attention(dec_out)                    # pondération par attention
        dec_out = self.decoder_dropout(dec_out)
        out = self.output_fc(dec_out)                        # (batch, seq_len, n_features)

        return out

    def reconstruction_error(self, x: torch.Tensor) -> torch.Tensor:
        """MSE par séquence — utilisé comme score d'anomalie."""
        with torch.no_grad():
            out = self.forward(x)
            return ((x - out) ** 2).mean(dim=(1, 2))        # (batch,)

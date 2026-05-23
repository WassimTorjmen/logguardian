"""
LSTM Autoencoder pour la détection d'anomalies dans les logs.

Principe : entraîné uniquement sur des logs normaux.
L'erreur de reconstruction (MSE) est faible pour les séquences normales
et élevée pour les séquences anormales.
"""
import torch
import torch.nn as nn


class LSTMAutoencoder(nn.Module):
    def __init__(
        self,
        n_features: int,
        hidden_size: int,
        latent_size: int,
        seq_len: int,
        num_layers: int = 1,
    ):
        super().__init__()
        self.seq_len = seq_len

        # Encodeur : LSTM → hidden state → vecteur latent
        self.encoder_lstm = nn.LSTM(
            input_size=n_features,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
        )
        self.encoder_fc = nn.Linear(hidden_size, latent_size)

        # Décodeur : latent → expansion → LSTM → reconstruction
        self.decoder_fc = nn.Linear(latent_size, hidden_size)
        self.decoder_lstm = nn.LSTM(
            input_size=hidden_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
        )
        self.output_fc = nn.Linear(hidden_size, n_features)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x : (batch, seq_len, n_features)

        # Encodage
        _, (h_n, _) = self.encoder_lstm(x)
        latent = self.encoder_fc(h_n[-1])                              # (batch, latent_size)

        # Décodage
        dec = self.decoder_fc(latent)                                   # (batch, hidden_size)
        dec = dec.unsqueeze(1).repeat(1, self.seq_len, 1)              # (batch, seq_len, hidden_size)
        dec_out, _ = self.decoder_lstm(dec)                            # (batch, seq_len, hidden_size)
        out = self.output_fc(dec_out)                                  # (batch, seq_len, n_features)

        return out

    def reconstruction_error(self, x: torch.Tensor) -> torch.Tensor:
        """MSE par séquence — utilisé comme score d'anomalie."""
        with torch.no_grad():
            out = self.forward(x)
            return ((x - out) ** 2).mean(dim=(1, 2))                  # (batch,)

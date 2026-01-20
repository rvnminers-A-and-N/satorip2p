"""
satorip2p/protocol/prediction_protocol.py

Decentralized prediction sharing protocol.

Allows predictors to share their predictions via P2P network:
- Predictions are published to stream-specific GossipSub topics
- Predictions are signed for verification
- Scoring/accuracy tracking is decentralized

Works alongside central server in hybrid mode:
- Central mode: Not used (central server collects predictions)
- Hybrid mode: Publishes to P2P AND central server
- P2P mode: Only P2P prediction sharing

Usage:
    from satorip2p.protocol.prediction_protocol import PredictionProtocol

    protocol = PredictionProtocol(peers)
    await protocol.start()

    # Subscribe to predictions for a stream
    await protocol.subscribe_to_predictions(stream_id, callback)

    # Publish a prediction
    await protocol.publish_prediction(stream_id, value, target_time)
"""

import logging
import time
import json
import hashlib
import trio
from typing import Dict, List, Optional, Callable, TYPE_CHECKING, Any, Union
from dataclasses import dataclass, field, asdict

if TYPE_CHECKING:
    from ..peers import Peers

logger = logging.getLogger("satorip2p.protocol.prediction_protocol")


@dataclass
class Prediction:
    """
    A prediction for a future observation.

    Predictions are made by predictors and scored based on accuracy
    once the actual observation arrives.
    """
    stream_id: str              # Which stream this predicts
    value: Union[float, str]    # Predicted value
    target_time: int            # When the prediction is for (Unix timestamp)
    predictor: str              # Evrmore address of predictor
    created_at: int             # When prediction was made
    hash: str = ""              # Hash of the prediction
    signature: str = ""         # Signed by predictor's wallet
    confidence: float = 0.0     # Predictor's confidence (0-1)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Generate hash if not provided."""
        if not self.hash:
            self.hash = self.compute_hash()

    def compute_hash(self) -> str:
        """Compute deterministic hash of prediction."""
        data = f"{self.stream_id}:{self.value}:{self.target_time}:{self.predictor}:{self.created_at}"
        return hashlib.sha256(data.encode()).hexdigest()[:32]

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Prediction":
        """Create from dictionary."""
        data.setdefault("metadata", {})
        data.setdefault("confidence", 0.0)
        return cls(**data)

    def get_signing_message(self) -> str:
        """Get the message that should be signed."""
        return f"{self.stream_id}:{self.value}:{self.target_time}:{self.hash}"


@dataclass
class PredictionScore:
    """
    Score for a prediction after actual observation arrives.

    Scores are computed by comparing predictions to actual values.
    """
    prediction_hash: str        # Hash of the prediction being scored
    stream_id: str              # Stream this is for
    predictor: str              # Who made the prediction
    predicted_value: float      # What they predicted
    actual_value: float         # What actually happened
    target_time: int            # When prediction was for
    score: float                # Accuracy score (0-1)
    scorer: str                 # Who computed this score
    timestamp: int              # When scored
    signature: str = ""         # Signed by scorer

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "PredictionScore":
        """Create from dictionary."""
        return cls(**data)


class PredictionProtocol:
    """
    Decentralized prediction sharing protocol.

    Allows predictors to publish predictions and receive predictions
    from other predictors without a central server.

    Architecture:
    - Each stream has prediction topic: satori/predictions/{stream_id}
    - Predictions are signed and broadcast via GossipSub
    - Scoring happens when actual observation arrives
    - Historical scores determine predictor reputation
    """

    # Topic prefix for predictions
    PREDICTION_TOPIC_PREFIX = "satori/predictions/"

    # Topic for prediction scores
    SCORE_TOPIC = "satori/prediction-scores"

    # Maximum predictions to cache per stream
    MAX_CACHE_SIZE = 1000

    def __init__(self, peers: "Peers"):
        """
        Initialize PredictionProtocol.

        Args:
            peers: Peers instance for P2P operations
        """
        self.peers = peers
        self._subscribed_streams: Dict[str, List[Callable]] = {}  # stream_id -> callbacks
        self._my_predictions: Dict[str, List[Prediction]] = {}  # stream_id -> my predictions
        self._prediction_cache: Dict[str, List[Prediction]] = {}  # stream_id -> all predictions
        self._score_cache: Dict[str, List[PredictionScore]] = {}  # stream_id -> scores
        self._score_callbacks: List[Callable] = []
        self._started = False

        # External callback for bridge integration (set by p2p_bridge)
        self.on_prediction_received: Optional[Callable[[Prediction], None]] = None

    @property
    def evrmore_address(self) -> str:
        """Get our Evrmore address."""
        if self.peers._identity_bridge:
            return self.peers._identity_bridge.evrmore_address
        return ""

    @property
    def peer_id(self) -> str:
        """Get our peer ID."""
        return self.peers.peer_id or ""

    async def start(self) -> bool:
        """
        Start the prediction protocol.

        Returns:
            True if started successfully
        """
        if self._started:
            return True

        try:
            # Subscribe to score announcements with full network registration
            if self.peers._pubsub:
                await self.peers.subscribe_async(
                    self.SCORE_TOPIC,
                    self._on_score_received
                )
                logger.debug(f"Subscribed to {self.SCORE_TOPIC}")

            self._started = True
            logger.info("PredictionProtocol started")
            return True

        except Exception as e:
            logger.error(f"Failed to start PredictionProtocol: {e}")
            return False

    async def stop(self) -> None:
        """Stop the prediction protocol."""
        # Unsubscribe from all prediction topics
        for stream_id in list(self._subscribed_streams.keys()):
            await self.unsubscribe_from_predictions(stream_id)

        # Unsubscribe from scores
        if self.peers._pubsub:
            try:
                await self.peers.unsubscribe(self.SCORE_TOPIC)
            except Exception:
                pass

        self._started = False
        logger.info("PredictionProtocol stopped")

    # ========== Prediction Subscription ==========

    async def subscribe_to_predictions(
        self,
        stream_id: str,
        callback: Callable[[Prediction], None]
    ) -> bool:
        """
        Subscribe to receive predictions for a stream.

        Args:
            stream_id: Stream to subscribe to
            callback: Function called with each Prediction

        Returns:
            True if subscribed successfully
        """
        topic = f"{self.PREDICTION_TOPIC_PREFIX}{stream_id}"

        # Track callback
        if stream_id not in self._subscribed_streams:
            self._subscribed_streams[stream_id] = []

            # Subscribe to GossipSub topic with full network registration
            if self.peers._pubsub:
                await self.peers.subscribe_async(
                    topic,
                    lambda data: self._on_prediction_received(stream_id, data)
                )

        self._subscribed_streams[stream_id].append(callback)
        logger.debug(f"Subscribed to predictions for {stream_id}")
        return True

    async def unsubscribe_from_predictions(self, stream_id: str) -> bool:
        """
        Unsubscribe from predictions for a stream.

        Args:
            stream_id: Stream to unsubscribe from

        Returns:
            True if unsubscribed successfully
        """
        if stream_id not in self._subscribed_streams:
            return False

        topic = f"{self.PREDICTION_TOPIC_PREFIX}{stream_id}"

        # Unsubscribe from GossipSub
        if self.peers._pubsub:
            try:
                await self.peers.unsubscribe(topic)
            except Exception:
                pass

        del self._subscribed_streams[stream_id]
        logger.debug(f"Unsubscribed from predictions for {stream_id}")
        return True

    async def _on_prediction_received(self, stream_id: str, data: dict) -> None:
        """Handle received prediction."""
        try:
            prediction = Prediction.from_dict(data)

            # Verify hash
            expected_hash = prediction.compute_hash()
            if prediction.hash != expected_hash:
                logger.debug(f"Invalid prediction hash from {prediction.predictor}")
                return

            # Verify signature
            if not await self._verify_prediction(prediction):
                logger.debug(f"Invalid prediction signature from {prediction.predictor}")
                return

            # Cache prediction
            if stream_id not in self._prediction_cache:
                self._prediction_cache[stream_id] = []
            self._prediction_cache[stream_id].append(prediction)

            # Trim cache
            if len(self._prediction_cache[stream_id]) > self.MAX_CACHE_SIZE:
                self._prediction_cache[stream_id] = self._prediction_cache[stream_id][-self.MAX_CACHE_SIZE:]

            # Notify stream-specific callbacks
            if stream_id in self._subscribed_streams:
                for callback in self._subscribed_streams[stream_id]:
                    try:
                        callback(prediction)
                    except Exception as e:
                        logger.debug(f"Prediction callback error: {e}")

            # Notify global callback (for p2p_bridge integration)
            if self.on_prediction_received:
                try:
                    self.on_prediction_received(prediction)
                except Exception as e:
                    logger.debug(f"Global prediction callback error: {e}")

            logger.debug(
                f"Received prediction for {stream_id} "
                f"value={prediction.value} from {prediction.predictor}"
            )

        except Exception as e:
            logger.debug(f"Failed to process prediction: {e}")

    async def _verify_prediction(self, prediction: Prediction) -> bool:
        """Verify a prediction's signature."""
        if prediction.signature == "unsigned":
            return True  # Development mode

        if not prediction.signature:
            return False

        try:
            message = prediction.get_signing_message()
            if self.peers._identity_bridge:
                return self.peers._identity_bridge.verify(
                    message.encode(),
                    prediction.signature.encode() if isinstance(prediction.signature, str) else prediction.signature,
                    prediction.predictor
                )
        except Exception:
            pass

        return False

    # ========== Prediction Publishing ==========

    async def publish_prediction(
        self,
        stream_id: str,
        value: Union[float, str],
        target_time: int,
        confidence: float = 0.0,
        metadata: dict = None
    ) -> Optional[Prediction]:
        """
        Publish a prediction for a future observation.

        Args:
            stream_id: Stream to predict
            value: Predicted value
            target_time: When this prediction is for
            confidence: Confidence level (0-1)
            metadata: Additional metadata

        Returns:
            Prediction if published successfully
        """
        if not self.evrmore_address:
            logger.warning("Cannot publish prediction: no evrmore address")
            return None

        prediction = Prediction(
            stream_id=stream_id,
            value=value,
            target_time=target_time,
            predictor=self.evrmore_address,
            created_at=int(time.time()),
            confidence=confidence,
            metadata=metadata or {},
        )

        # Sign the prediction
        try:
            message = prediction.get_signing_message()
            if self.peers._identity_bridge:
                signature = self.peers._identity_bridge.sign(message.encode())
                prediction.signature = signature if isinstance(signature, str) else signature.decode()
        except Exception as e:
            logger.warning(f"Failed to sign prediction: {e}")
            prediction.signature = "unsigned"

        # Store our prediction
        if stream_id not in self._my_predictions:
            self._my_predictions[stream_id] = []
        self._my_predictions[stream_id].append(prediction)

        # Broadcast prediction
        topic = f"{self.PREDICTION_TOPIC_PREFIX}{stream_id}"
        try:
            await self.peers.broadcast(topic, prediction.to_dict())
            logger.debug(f"Published prediction for {stream_id} value={value}")
            return prediction
        except Exception as e:
            logger.warning(f"Failed to publish prediction: {e}")
            return None

    # ========== Scoring ==========

    async def score_prediction(
        self,
        prediction: Prediction,
        actual_value: float
    ) -> Optional[PredictionScore]:
        """
        Score a prediction based on actual observation.

        Args:
            prediction: Prediction to score
            actual_value: Actual observed value

        Returns:
            PredictionScore if successful
        """
        # Compute score (simple absolute error, normalized)
        try:
            predicted = float(prediction.value)
            actual = float(actual_value)
            error = abs(predicted - actual)
            # Normalize error to 0-1 score (higher is better)
            # Using exponential decay: score = e^(-error/scale)
            import math
            scale = max(abs(actual), 1.0) * 0.1  # 10% of actual value
            score = math.exp(-error / scale)
        except (ValueError, TypeError):
            score = 0.0

        prediction_score = PredictionScore(
            prediction_hash=prediction.hash,
            stream_id=prediction.stream_id,
            predictor=prediction.predictor,
            predicted_value=float(prediction.value) if isinstance(prediction.value, (int, float)) else 0,
            actual_value=actual_value,
            target_time=prediction.target_time,
            score=score,
            scorer=self.evrmore_address,
            timestamp=int(time.time()),
        )

        # Sign the score
        try:
            message = f"{prediction.hash}:{score}:{self.evrmore_address}"
            if self.peers._identity_bridge:
                signature = self.peers._identity_bridge.sign(message.encode())
                prediction_score.signature = signature if isinstance(signature, str) else signature.decode()
        except Exception as e:
            logger.warning(f"Failed to sign score: {e}")
            prediction_score.signature = "unsigned"

        # Store score
        stream_id = prediction.stream_id
        if stream_id not in self._score_cache:
            self._score_cache[stream_id] = []
        self._score_cache[stream_id].append(prediction_score)

        # Broadcast score
        try:
            await self.peers.broadcast(self.SCORE_TOPIC, prediction_score.to_dict())
            logger.debug(
                f"Scored prediction {prediction.hash} "
                f"score={score:.3f}"
            )
            return prediction_score
        except Exception as e:
            logger.warning(f"Failed to broadcast score: {e}")
            return prediction_score

    async def _on_score_received(self, data: dict) -> None:
        """Handle received prediction score."""
        try:
            score = PredictionScore.from_dict(data)

            # Don't process our own scores
            if score.scorer == self.evrmore_address:
                return

            # Store score
            stream_id = score.stream_id
            if stream_id not in self._score_cache:
                self._score_cache[stream_id] = []
            self._score_cache[stream_id].append(score)

            logger.debug(
                f"Received score for {score.prediction_hash} "
                f"score={score.score:.3f}"
            )

            # Notify callbacks
            for callback in self._score_callbacks:
                try:
                    callback(score)
                except Exception as e:
                    logger.debug(f"Score callback error: {e}")

        except Exception as e:
            logger.debug(f"Failed to process score: {e}")

    def on_score_received(self, callback: Callable[[PredictionScore], None]) -> None:
        """Register callback for new prediction scores."""
        self._score_callbacks.append(callback)

    # ========== Query Methods ==========

    def get_cached_predictions(
        self,
        stream_id: str,
        limit: int = 100
    ) -> List[Prediction]:
        """Get cached predictions for a stream."""
        predictions = self._prediction_cache.get(stream_id, [])
        return predictions[-limit:]

    def get_my_predictions(
        self,
        stream_id: str = None
    ) -> List[Prediction]:
        """Get our predictions, optionally filtered by stream."""
        if stream_id:
            return self._my_predictions.get(stream_id, [])
        else:
            all_predictions = []
            for preds in self._my_predictions.values():
                all_predictions.extend(preds)
            return all_predictions

    def get_predictions_for_time(
        self,
        stream_id: str,
        target_time: int,
        tolerance: int = 60
    ) -> List[Prediction]:
        """Get predictions targeting a specific time window."""
        predictions = self._prediction_cache.get(stream_id, [])
        return [
            p for p in predictions
            if abs(p.target_time - target_time) <= tolerance
        ]

    def get_predictor_scores(
        self,
        predictor: str,
        stream_id: str = None
    ) -> List[PredictionScore]:
        """Get scores for a specific predictor."""
        scores = []
        if stream_id:
            scores = [s for s in self._score_cache.get(stream_id, []) if s.predictor == predictor]
        else:
            for stream_scores in self._score_cache.values():
                scores.extend([s for s in stream_scores if s.predictor == predictor])
        return scores

    def get_predictor_average_score(
        self,
        predictor: str,
        stream_id: str = None
    ) -> float:
        """Get average score for a predictor."""
        scores = self.get_predictor_scores(predictor, stream_id)
        if not scores:
            return 0.0
        return sum(s.score for s in scores) / len(scores)

    # ========== Statistics ==========

    def get_stats(self) -> dict:
        """Get prediction protocol statistics."""
        return {
            "subscribed_streams": len(self._subscribed_streams),
            "my_predictions": sum(len(p) for p in self._my_predictions.values()),
            "cached_predictions": sum(len(p) for p in self._prediction_cache.values()),
            "cached_scores": sum(len(s) for s in self._score_cache.values()),
            "started": self._started,
        }

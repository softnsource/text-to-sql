"""Result merger - combines results from multiple service queries using pandas."""

import logging
from dataclasses import dataclass
from typing import Any, List, Optional, Dict, cast

import pandas as pd

from app.config import Settings, get_settings
from app.query.executor import QueryResult
from app.exceptions import QueryError


logger = logging.getLogger(__name__)


@dataclass
class MergedResult:
    """Merged result from combining multiple service queries."""
    data: List[Dict[str, Any]]  # Merged rows as list of dicts
    columns: List[str]  # Column names
    row_count: int
    services_used: List[str]
    merge_applied: bool
    was_truncated: bool = False
    truncation_reason: Optional[str] = None


class ResultMerger:
    """Merges results from multiple services."""

    def __init__(self, settings: Optional[Settings] = None):
        """Initialize result merger.

        Args:
            settings: Application settings (uses get_settings() if None)
        """
        self.settings = settings or get_settings()

    def merge(
        self,
        results: List[QueryResult],
        merge_strategy: Optional[Dict[str, str]] = None
    ) -> MergedResult:
        """Merge results from multiple services.

        Args:
            results: List of QueryResult objects from different services
            merge_strategy: Optional merge strategy with join_column and join_type

        Returns:
            MergedResult with combined data
        """
        logger.info(f"Merging {len(results)} query results")

        # Filter out results with errors
        valid_results = [r for r in results if not r.error]

        if not valid_results:
            logger.warning("No valid results to merge")
            return MergedResult(
                data=[],
                columns=[],
                row_count=0,
                services_used=[r.service_name for r in results],
                merge_applied=False
            )

        # Single result - no merge needed
        if len(valid_results) == 1:
            result = valid_results[0]
            return MergedResult(
                data=result.rows,
                columns=result.columns,
                row_count=result.row_count,
                services_used=[result.service_name],
                merge_applied=False
            )

        # Multiple results - merge required
        try:
            join_column = merge_strategy.get("join_column") if merge_strategy else None
            if join_column and isinstance(join_column, str):
                assert merge_strategy is not None
                merged_data = self._merge_with_join(valid_results, merge_strategy)
            else:
                if merge_strategy and merge_strategy.get("join_column"):
                    logger.warning(
                        f"Invalid join column '{merge_strategy.get('join_column')}', "
                        f"falling back to concatenation"
                    )
                merged_data = self._concatenate_results(valid_results)

            # Enforce max_merged_rows limit
            was_truncated = False
            truncation_reason = None
            max_rows = self.settings.query.max_merged_rows
            if len(merged_data) > max_rows:
                logger.warning(
                    f"Merged result has {len(merged_data)} rows, "
                    f"truncating to {max_rows}"
                )
                was_truncated = True
                truncation_reason = f"Result exceeded {max_rows} row limit (had {len(merged_data)} rows)"
                merged_data = merged_data[:max_rows]

            # Convert DataFrame to list of dicts
            assert isinstance(merged_data, pd.DataFrame)
            data: List[Dict[str, Any]] = cast(
                List[Dict[str, Any]],
                merged_data.to_dict(orient="records")
            )
            columns = list(merged_data.columns)

            return MergedResult(
                data=data,
                columns=columns,
                row_count=len(data),
                services_used=[r.service_name for r in valid_results],
                merge_applied=True,
                was_truncated=was_truncated,
                truncation_reason=truncation_reason
            )

        except Exception as e:
            logger.error(f"Failed to merge results: {e}", exc_info=True)
            raise QueryError("Unable to combine query results. Please try a simpler query.", str(e))

    def _merge_with_join(
        self,
        results: List[QueryResult],
        merge_strategy: Dict[str, str]
    ) -> pd.DataFrame:
        """Merge results using pandas join.

        Args:
            results: List of QueryResult objects
            merge_strategy: Join strategy with join_column and join_type

        Returns:
            Merged pandas DataFrame
        """
        join_column = merge_strategy.get("join_column")
        join_type = merge_strategy.get("join_type", "inner")

        # Map join_type to pandas 'how' parameter
        how = join_type.lower()
        if how not in ("inner", "left", "right", "outer"):
            logger.warning(f"Invalid join type '{join_type}', using 'inner'")
            how = "inner"

        logger.info(f"Merging results on column '{join_column}' using '{how}' join")

        # Convert first result to DataFrame
        df = pd.DataFrame(results[0].rows)

        # Join with remaining results
        for result in results[1:]:
            right_df = pd.DataFrame(result.rows)

            # Check if join column exists in both DataFrames
            if join_column not in df.columns:
                logger.warning(
                    f"Join column '{join_column}' not found in left DataFrame, "
                    f"available columns: {list(df.columns)}"
                )
                continue

            if join_column not in right_df.columns:
                logger.warning(
                    f"Join column '{join_column}' not found in right DataFrame, "
                    f"available columns: {list(right_df.columns)}"
                )
                continue

            # Perform join
            try:
                df = df.merge(
                    right_df,
                    on=join_column,
                    how=how,
                    suffixes=('', f'_{result.service_name}')
                )
                logger.debug(f"Joined with {result.service_name}, result shape: {df.shape}")
            except Exception as e:
                logger.error(f"Failed to join with {result.service_name}: {e}")
                continue

        return df

    def _concatenate_results(self, results: List[QueryResult]) -> pd.DataFrame:
        logger.info("Concatenating results without join")
        dataframes = [pd.DataFrame(result.rows) for result in results]
        merged = pd.concat(dataframes, ignore_index=True)
        
        # Narrow type from Series | DataFrame to DataFrame explicitly
        if not isinstance(merged, pd.DataFrame):
            merged = pd.DataFrame(merged)
        
        return merged

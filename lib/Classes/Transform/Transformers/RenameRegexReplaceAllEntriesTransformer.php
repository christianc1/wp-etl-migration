<?php
/**
 * Transformer: RenameRegexReplaceAllEntriesTransformer
 *
 * Renames all entries in a row by applying a regex pattern replacement to their names.
 *
 * @package TenupETL\Classes\Transform\Transformers
 */

declare(strict_types=1);

namespace TenupETL\Classes\Transform\Transformers;

use Flow\ETL\{FlowContext, Row, Rows, Transformer};

/**
 * Transformer that renames entries using regex pattern matching.
 */
final class RenameRegexReplaceAllEntriesTransformer implements Transformer {
	/**
	 * Constructor.
	 *
	 * @param string $regex   The regex pattern to match.
	 * @param string $replace The replacement pattern.
	 */
	public function __construct(
		private readonly string $regex,
		private readonly string $replace,
	) {}

	/**
	 * Transform the rows by renaming entries according to the regex pattern.
	 *
	 * @param Rows        $rows    The rows to transform.
	 * @param FlowContext $context The flow context.
	 * @return Rows The transformed rows.
	 */
	public function transform( Rows $rows, FlowContext $context ): Rows {
		return $rows->map(
			function ( Row $row ): Row {
				foreach ( $row->entries()->all() as $entry ) {
					$row = $row->rename( $entry->name(), \preg_replace( $this->regex, $this->replace, $entry->name() ) );
				}

				return $row;
			}
		);
	}
}

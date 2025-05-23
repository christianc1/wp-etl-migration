<?php
/**
 * Class: WordPress Media Loader
 *
 * Loads media into WordPress from ETL rows.
 *
 * @package TenupETL\Classes\Load\Loaders
 */

namespace TenupETL\Classes\Load\Loaders;

use TenupETL\Utils\{ WithLogging, WithSideLoadMedia };
use Flow\ETL\{FlowContext, Loader, Rows, Row};
use function Flow\ETL\DSL\{integer_entry, string_entry, rows_to_array};
use TenupETL\Classes\Config\GlobalConfig;
use Flow\ETL\Adapter\WordPress\Loaders\{WPPostsLoader, WPPostMetaLoader, WPMediaLoader};

/**
 * Class WordPressMediaLoader
 *
 * Handles loading media into WordPress during ETL process.
 */
class WordPressMediaLoader extends BaseLoader implements Loader, RowMutator {
	use WithLogging;
	use WithSideLoadMedia;
	use WithRowMutation;

	/**
	 * The adapter to use for loading media
	 *
	 * @var WPMediaLoader
	 */
	protected $media_adapter;

	public function __construct(
		protected array $step_config,
		protected GlobalConfig $global_config,
	) {
		parent::__construct( $step_config, $global_config );

		$this->media_adapter = new WPMediaLoader( $step_config['args'] ?? [] );
		$this->media_adapter->withDateTimeFormat( \DateTimeInterface::ATOM );
	}

	/**
	 * Run the loader
	 *
	 * @param object $state The ETL state object.
	 * @return void
	 */
	public function run( $state ) {
		$state
			->write(
				$this
			);

		$this->log( 'Loading media with WordPress core apis.', 'progress' );
	}

	/**
	 * Load rows into WordPress as posts
	 *
	 * @param Rows        $rows    The rows to load.
	 * @param FlowContext $context The flow context.
	 * @return void
	 */
	public function load( Rows $rows, FlowContext $context ): void {
		$normalizer = $this->media_adapter->create_normalizer( $context );

		foreach ( $rows as $row ) {
			try {
				// Get attachment IDs from media loader
				$attachments = $this->media_adapter->insertMedia( $row, normalizer: $normalizer );

				// Replace placeholders with actual attachment IDs
				$updated_row = $this->replaceMediaPlaceholders( $row, $attachments );

				// Mutate the row with the updated values
				$row = $this->mutate_row( $updated_row );
			} catch ( \Exception $e ) {
				// Even if media loading fails, we still want to replace placeholders with empty values
				$updated_row = $this->replaceMediaPlaceholders( $row, [] );
				$row = $this->mutate_row( $updated_row );
				continue;
			}
		}
	}

	/**
	 * Replace media placeholders in row entries with actual attachment IDs
	 *
	 * @param Row $row Original row
	 * @param array $attachments Array of attachment IDs keyed by media type
	 * @return Row Updated row with replaced values
	 */
	protected function replaceMediaPlaceholders( Row $row, array $attachments ): Row {
		$updated_row = $row;

		foreach ( $row->entries() as $entry ) {
			$value = $entry->value();

			// Skip if not a string or doesn't contain placeholder pattern
			if ( ! is_string( $value ) || ! str_contains( $value, '%%' ) ) {
				continue;
			}

			// Find all placeholders matching %%...%%
			if ( preg_match_all( '/%%([^%]+)%%/', $value, $matches ) ) {
				$new_value = $value;

				foreach ( $matches[1] as $placeholder ) {
					// The placeholder variable contains the full key (e.g., 'media.featured_image.url')
					if ( isset( $attachments[ $placeholder ] ) ) {
						// Replace placeholder with the actual value (ID or URL) from attachments array
						$new_value = str_replace(
							"%%{$placeholder}%%",
							(string) $attachments[ $placeholder ],
							$new_value
						);
					} else {
						// Replace placeholder with empty string if value not found in attachments
						$new_value = str_replace(
							"%%{$placeholder}%%",
							'',
							$new_value
						);
					}
				}

				// Only update if replacements were made
				if ( $new_value !== $value ) {
					// Create appropriate entry type based on context
					// Convert to integer if the final value is purely numeric and the placeholder indicates an ID
					if ( ctype_digit( $new_value ) && ! empty( $new_value ) && str_ends_with( $placeholder, '.attachment_id' ) ) {
						$new_entry = integer_entry( $entry->name(), (int) $new_value );
					} else {
						$new_entry = string_entry( $entry->name(), $new_value );
					}
					$updated_row = $updated_row->remove( $entry->name() )->add( $new_entry );
				} elseif ( empty( $new_value ) && $value !== $new_value ) {
					// Also remove the entry if the placeholder existed but replacement resulted in an empty string
					$updated_row = $updated_row->remove( $entry->name() );
				}
			}
		}

		return $updated_row;
	}
}

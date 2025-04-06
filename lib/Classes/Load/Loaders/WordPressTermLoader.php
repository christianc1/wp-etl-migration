<?php
/**
 * Class: WordPress Term Loader
 *
 * Loads terms into WordPress from ETL rows.
 *
 * @package TenupETL\Classes\Load\Loaders
 */

namespace TenupETL\Classes\Load\Loaders;

use TenupETL\Utils\{ WithLogging, WithSideLoadMedia };

use Flow\ETL\{FlowContext, Loader, Rows, Row};
use function Flow\ETL\DSL\{integer_entry, rows_to_array};
use TenupETL\Classes\Config\GlobalConfig;
use Flow\ETL\Adapter\WordPress\Loaders\{WPTermsLoader};

/**
 * Class WordPressPostLoader
 *
 * Handles loading posts into WordPress during ETL process.
 */
class WordPressTermLoader extends BaseLoader implements Loader, RowMutator {
	use WithLogging;
	use WithRowMutation;

	/**
	 * The adapter to use for loading terms
	 *
	 * @var WPTermsLoader
	 */
	protected $adapter;

	/**
	 * Keep track of the last normalized data for garbage collection
	 *
	 * @var array|null
	 */
	private $last_normalized_data = null;

	public function __construct(
		protected array $step_config,
		protected GlobalConfig $global_config,
	) {
		parent::__construct( $step_config, $global_config );

		$this->adapter = new WPTermsLoader( $step_config['args'] ?? [] );
		$this->adapter->withDateTimeFormat( \DateTimeInterface::ATOM );
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

		$this->log( 'Loading terms with WordPress core apis.', 'progress' );
	}

	/**
	 * Load rows into WordPress as posts
	 *
	 * @param Rows        $rows    The rows to load.
	 * @param FlowContext $context The flow context.
	 * @return void
	 */
	public function load( Rows $rows, FlowContext $context ): void {
		$normalizer = $this->adapter->create_normalizer( $context );
		$processed_count = 0;
		$memory_cleanup_interval = 10; // Clean up memory every X posts

		foreach ( $rows as $row ) {
			try {
				$this->log( 'Processing term: ' . $row->valueOf( 'term.name' ), 'progress' );
				if ( isset( $this->step_config['upsert'] ) && $this->step_config['upsert'] ) {
					// Check for existing post
					$existing_term = $this->term_exists( $row );
					if ( $existing_term ) {
						$this->log( 'Updating term: ' . $row->valueOf( 'term.name' ), 'progress' );

						$row = $this->mutate_row( $row->add( integer_entry( 'term.term_id', $existing_term ) ) );
					}
				}
				$term_id = $this->adapter->insertTerm( $row, normalizer: $normalizer );
			} catch ( \Exception $e ) {
				if ( isset ( $this->step_config['upsert'] ) && $this->step_config['upsert'] ) {
					$this->log( 'Error upserting term: ' . $row->valueOf( 'term.name' ), 'warning' );
					$this->log( $e->getMessage(), 'warning' );
					continue;
				}

				$this->log( 'Error inserting term: ' . $row->valueOf( 'term.name' ), 'warning' );
				$this->log( $e->getMessage(), 'warning' );
				$this->mutate_row( $row->add( integer_entry( 'term.term_id', 0 ) ) );
				continue;
			}

			// Add term_id to the row
			if ( ! $row->has( 'term.term_id' ) ) {
				$row = $this->mutate_row( $row->add( integer_entry( 'term.term_id', $term_id ) ) );
			}
		}
	}

	/**
	 * Check if the term exists
	 *
	 * @param Row $row The row to check.
	 * @return int|false The term ID if it exists, false otherwise.
	 */
	protected function term_exists( Row $row ): int {
		$term_id = $row->has( 'term.term_id' ) ? $row->valueOf( 'term.term_id' ) : 0;
		if ( $term_id ) {
			return $term_id;
		}

		if ( $row->has( 'term.taxonomy' ) ) {
			$term_taxonomy = $row->valueOf( 'term.taxonomy' );
		} else {
			return 0;
		}

		if ( $row->has( 'term.slug' ) ) {
			$term_slug = $row->valueOf( 'term.slug' );
			$term = get_term_by( 'slug', $term_slug, $term_taxonomy );
		}

		if ( $row->has( 'term.name' ) ) {
			$term_name = $row->valueOf( 'term.name' );
			$term = get_term_by( 'name', $term_name, $term_taxonomy );
		}

		if ( $term ) {
			return $term->term_id;
		}

		return 0;
	}
}

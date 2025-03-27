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

use Flow\ETL\{FlowContext, Loader, Rows};
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

		$this->terms_adapter = new WPTermsLoader( $step_config['args'] ?? [] );
		$this->terms_adapter->withDateTimeFormat( \DateTimeInterface::ATOM );
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
		$normalizer = $this->terms_adapter->create_normalizer( $context );
		$processed_count = 0;
		$memory_cleanup_interval = 10; // Clean up memory every X posts

		foreach ( $rows as $row ) {
			try {
				$term_id = $this->terms_adapter->insertTerm( $row, normalizer: $normalizer );
			} catch ( \Exception $e ) {
				$this->log( 'Error inserting term: ' . $row->valueOf( 'term.name' ), 'warning' );
				$this->log( $e->getMessage(), 'warning' );
				$this->mutate_row( $row->add( integer_entry( 'term.term_id', 0 ) ) );
				continue;
			}

			// Add term_id to the row
			$row = $this->mutate_row( $row->add( integer_entry( 'term.term_id', $term_id ) ) );
		}
	}
}

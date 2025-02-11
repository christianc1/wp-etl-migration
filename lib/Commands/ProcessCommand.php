<?php
/**
 * Class ProcessCommand
 *
 * This class extends the BaseCommand and processes a migration.
 *
 * @package TenupETL\Commands
 */

namespace TenupETL\Commands;

use TenupETL\Classes\Pipeline\Pipeline;
use TenupETL\Utils\WithLogging;
use WP_CLI;

/**
 * Manages ETL content migration processes.
 *
 * ## EXAMPLES
 *
 *     # Process all configured migrations
 *     $ wp etl process
 *
 *     # Process specific migration type
 *     $ wp etl process --type=articles
 *
 *     # Process with debug output
 *     $ wp etl process --debug
 */
class ProcessCommand extends BaseCommand {
	use WithLogging;

	/**
	 * Migration statistics.
	 *
	 * @var array<string,int>
	 */
	protected $stats = [
		'processed' => 0,
		'succeeded' => 0,
		'failed'    => 0,
		'skipped'   => 0,
	];

	/**
	 * Process the migration pipeline.
	 *
	 * ## OPTIONS
	 *
	 * [--type=<type>]
	 * : Only process specific migration type(s), comma separated
	 *
	 * [--skip=<type>]
	 * : Skip specific migration type(s), comma separated
	 *
	 * [--debug]
	 * : Enable verbose debug output
	 *
	 * [--dry-run]
	 * : Validate and show what would be processed without making changes
	 *
	 * [--show-progress-bar]
	 * : Show overall progress bar based on analysis data
	 *
	 * [--show-progress]
	 * : Show detailed progress messages during processing
	 *
	 * [--yes]
	 * : Skip confirmation prompts
	 *
	 * ## EXAMPLES
	 *
	 *     wp etl process --type=articles,media
	 *     wp etl process --skip=users --debug
	 *     wp etl process --dry-run
	 *
	 * @param array $args       Positional arguments.
	 * @param array $assoc_args Associative arguments.
	 * @return void
	 */
	public function process( $args, $assoc_args ) {
		$type              = \WP_CLI\Utils\get_flag_value( $assoc_args, 'type', null );
		$skip              = \WP_CLI\Utils\get_flag_value( $assoc_args, 'skip', null );
		$debug             = \WP_CLI\Utils\get_flag_value( $assoc_args, 'debug', false );
		$dry_run           = \WP_CLI\Utils\get_flag_value( $assoc_args, 'dry-run', false );
		$show_progress_bar = \WP_CLI\Utils\get_flag_value( $assoc_args, 'show-progress-bar', false );
		$show_progress     = \WP_CLI\Utils\get_flag_value( $assoc_args, 'show-progress', false );

		try {
			// Parse types to process/skip.
			$process_types = $type ? array_map( 'trim', explode( ',', $type ) ) : [];
			$skip_types    = $skip ? array_map( 'trim', explode( ',', $skip ) ) : [];

			// Filter config based on type/skip parameters.
			$filtered_config = $this->filter_migration_config( $process_types, $skip_types );

			if ( empty( $filtered_config ) ) {
				WP_CLI::error( 'No valid migration types found to process' );
			}

			// If showing progress bar, ensure we have analysis data
			if ( $show_progress_bar ) {
				$manifest = get_option( $this->config->get_value( 'slug' ) . '_migration_manifest', [] );
				$missing_analysis = false;

				foreach ( $filtered_config as $job ) {
					if ( ! isset( $manifest[ $job['name'] ] ) ) {
						$missing_analysis = true;
						break;
					}
				}

				if ( $missing_analysis ) {
					WP_CLI::log( 'Analysis data missing. Running analysis first...' );
					$this->analyze( [], [ 'force' => true ] );
					$manifest = get_option( $this->config->get_value( 'slug' ) . '_migration_manifest', [] );
				}

				// Calculate total rows to process
				$total_rows = array_sum(
					array_map(
						function ( $job ) use ( $manifest ) {
							return $manifest[ $job['name'] ]['count'] ?? 0;
						},
						$filtered_config
					)
				);

				if ( 0 === $total_rows ) {
					WP_CLI::warning( 'No rows found to process in analysis' );
					if ( ! \WP_CLI\Utils\get_flag_value( $assoc_args, 'yes', false ) ) {
						WP_CLI::confirm( 'Continue anyway?' );
					}
				}
			}

			// Show what will be processed.
			$this->preview_migrations( $filtered_config );

			if ( ! $dry_run && ! \WP_CLI\Utils\get_flag_value( $assoc_args, 'yes', false ) ) {
				WP_CLI::confirm( 'Proceed with migration?' );
			}

			if ( $dry_run ) {
				WP_CLI::success( 'Dry run complete. No changes made.' );
				return;
			}

			self::log( 'Reading pipeline configuration...', 'success' );
			$pipeline = new Pipeline( $this->config );

			self::log( 'Building pipeline...', 'success' );
			$pipeline->build();

			// Enable debug logging if requested.
			if ( $debug ) {
				add_filter( 'tenup_etl_debug_logging', '__return_true' );
			}

			// Control progress message display
			if ( ! $show_progress ) {
				add_filter( 'tenup_etl_show_progress', '__return_false' );
			}

			// Initialize progress bar if requested
			$progress = null;
			$processed_rows = 0;
			if ( $show_progress_bar && $total_rows > 0 ) {
				$progress = \WP_CLI\Utils\make_progress_bar(
					'Processing migrations',
					$total_rows
				);
			}

			self::start_bulk_operation();

			try {
				// Add callback to track progress
				if ( $progress ) {
					add_action(
						'tenup_etl_row_processed',
						function ( $count ) use ( &$processed_rows, $progress ) {
							$processed_rows += $count;
							$progress->tick( $count );
						}
					);
				}

				$pipeline->process();

				if ( $progress ) {
					$progress->finish();
				}
			} finally {
				self::end_bulk_operation();
			}

			WP_CLI::success( 'Migration complete!' );

		} catch ( \Exception $e ) {
			$this->handle_error( $e );
		}
	}

	/**
	 * Filter migration configuration based on types to process/skip.
	 *
	 * @param array $process_types Types to process.
	 * @param array $skip_types    Types to skip.
	 * @return array Filtered configuration.
	 */
	protected function filter_migration_config( array $process_types, array $skip_types ): array {
		$config = $this->config->get_value( 'migration' );

		return array_filter(
			$config,
			function ( $job ) use ( $process_types, $skip_types ) {
				// Skip if explicitly marked to skip in config.
				if ( ! empty( $job['skip'] ) ) {
					return false;
				}

				// Skip if in skip list.
				if ( in_array( $job['name'], $skip_types, true ) ) {
					return false;
				}

				// Include if no specific types requested or if in process list.
				return empty( $process_types ) || in_array( $job['name'], $process_types, true );
			}
		);
	}

	/**
	 * Show preview of migrations to be processed.
	 *
	 * @param array $migrations Array of migration configurations.
	 * @return void
	 */
	protected function preview_migrations( array $migrations ) {
		WP_CLI::log( "\nMigrations to process:" );

		$items = array_map(
			function ( $job ) {
				return [
					'name'         => $job['name'],
					'description'  => $job['description'] ?? 'No description',
					'dependencies' => implode( ', ', $job['depends_on'] ?? [] ),
				];
			},
			$migrations
		);

		\WP_CLI\Utils\format_items( 'table', $items, [ 'name', 'description', 'dependencies' ] );
	}

	/**
	 * Extract data from a job source.
	 *
	 * @param array $job The job configuration array.
	 * @return void
	 */
	protected function extract( $job ) {
		$extract = $job['extract'];
		self::log( 'Extracting data from ' . $extract['source'] );
	}

	/**
	 * Track progress of the migration.
	 *
	 * @param int $total   Total number of items.
	 * @param int $current Current item number.
	 * @return void
	 */
	protected function track_progress( $total, $current ) {
		static $progress;

		if ( ! $progress ) {
			$progress = \WP_CLI\Utils\make_progress_bar(
				'Processing items',
				$total
			);
		}

		$progress->tick();

		if ( $current === $total ) {
			$progress->finish();
		}
	}

	/**
	 * Save migration progress.
	 *
	 * @param string $job_id            Job identifier.
	 * @param int    $last_processed_id Last processed item ID.
	 * @return void
	 */
	protected function save_progress( $job_id, $last_processed_id ) {
		update_option( $this->config->get_value( 'slug' ) . "_migration_progress_{$job_id}", $last_processed_id );
	}

	/**
	 * Get last saved progress.
	 *
	 * @param string $job_id Job identifier.
	 * @return mixed
	 */
	protected function get_last_progress( $job_id ) {
		return get_option( $this->config->get_value( 'slug' ) . "_migration_progress_{$job_id}" );
	}

	/**
	 * Validate migration configuration.
	 *
	 * @param array $job Job configuration.
	 * @return void
	 */
	protected function validate_migration( $job ) {
		// Validate source data.
		if ( ! $this->validate_source( $job['source'] ) ) {
			WP_CLI::error( 'Invalid source data' );
		}

		// Validate destination.
		if ( ! $this->validate_destination( $job['destination'] ) ) {
			WP_CLI::error( 'Destination validation failed' );
		}
	}

	/**
	 * Create database snapshot.
	 *
	 * @return void
	 */
	protected function create_snapshot() {
		// Create DB snapshot before migration.
		WP_CLI::runcommand( 'db export migration-backup.sql' );
	}

	/**
	 * Rollback migration.
	 *
	 * @return void
	 */
	protected function rollback() {
		// Restore from snapshot.
		WP_CLI::runcommand( 'db import migration-backup.sql' );
	}

	/**
	 * Log migration statistics.
	 *
	 * @return void
	 */
	protected function log_migration_stats() {
		WP_CLI::log( "\nMigration Summary:" );
		WP_CLI::log( "Processed: {$this->stats['processed']}" );
		WP_CLI::log( "Succeeded: {$this->stats['succeeded']}" );
		WP_CLI::log( "Failed: {$this->stats['failed']}" );
		WP_CLI::log( "Skipped: {$this->stats['skipped']}" );
	}

	/**
	 * Analyzes source data and generates processing statistics.
	 *
	 * Creates a manifest of total counts and other metadata about the source data
	 * that can be used to provide progress information during processing.
	 *
	 * ## OPTIONS
	 *
	 * [--type=<type>]
	 * : Specific migration type to analyze (e.g. articles, events)
	 *
	 * [--force]
	 * : Force reanalysis even if manifest exists
	 *
	 * ## EXAMPLES
	 *
	 *     # Analyze all migration sources
	 *     $ wp etl analyze
	 *
	 *     # Analyze specific migration type
	 *     $ wp etl analyze --type=articles
	 *
	 * @param array $args       Positional arguments.
	 * @param array $assoc_args Associative arguments.
	 * @return void
	 */
	public function analyze( $args, $assoc_args ) {
		$type  = \WP_CLI\Utils\get_flag_value( $assoc_args, 'type', null );
		$force = \WP_CLI\Utils\get_flag_value( $assoc_args, 'force', false );

		if ( ! $force ) {
			$existing = get_option( $this->config->get_value( 'slug' ) . '_migration_manifest', [] );
			if ( ! empty( $existing ) ) {
				WP_CLI::log( 'Using existing manifest. Use --force to regenerate.' );
				WP_CLI::log( \WP_CLI\Utils\format_items( 'table', $existing, [ 'type', 'count', 'last_analyzed' ] ) );
				return;
			}
		}

		try {
			// Filter jobs based on type if specified.
			$jobs = $this->filter_migration_config(
				$type ? [ $type ] : [],
				[]
			);

			$manifest = [];

			foreach ( $jobs as $job_config ) {
				$job_name = $job_config['name'];

				if ( empty( $job_config['extract'] ) ) {
					WP_CLI::warning( "Skipping {$job_name} - no extract configuration found" );
					continue;
				}

				WP_CLI::log( "Analyzing {$job_name}..." );

				// Create extractor orchestrator directly.
				$extractor = new \TenupETL\Classes\Extract\Orchestrator(
					$this->config,
					new \TenupETL\Classes\Config\JobConfig( $job_config ),
					new \TenupETL\Classes\Load\LedgerRegistry( $this->config )
				);

				try {
					// Initialize state with empty DataFrame.
					$initial_state = \Flow\ETL\DSL\data_frame();

					// Process only the extraction phase.
					$extracted_state = $extractor->process( $initial_state );

					// Count rows from extracted state.
					$report = $extractor->get_current_state()
						->run( analyze: true );

				} catch ( \Exception $e ) {
					WP_CLI::warning( "Error analyzing {$job_name}: " . $e->getMessage() );
					continue;
				}

				$manifest[ $job_name ] = [
					'type'            => $job_name,
					'count'           => $report->statistics()->totalRows(),
					'time_to_analyze' => $report->statistics()->executionTime->highResolutionTime->toString(),
					'last_analyzed'   => current_time( 'mysql' ),
					'extract_config'  => $job_config['extract'],
					'source_type'     => $job_config['extract'][0]['adapter'] ?? 'unknown',
				];
			}

			// Store manifest with config slug
			update_option( $this->config->get_value( 'slug' ) . '_migration_manifest', $manifest );

			WP_CLI::success( 'Analysis complete!' );
			WP_CLI::log(
				\WP_CLI\Utils\format_items(
					'table',
					array_map(
						function ( $m ) {
							return [
								'type'            => $m['type'],
								'source'          => $m['source_type'],
								'count'           => $m['count'],
								'time_to_analyze' => $m['time_to_analyze'],
								'last_analyzed'   => $m['last_analyzed'],
							];
						},
						$manifest
					),
					[ 'type', 'source', 'count', 'time_to_analyze', 'last_analyzed' ]
				)
			);

		} catch ( \Exception $e ) {
			WP_CLI::error( 'Analysis failed: ' . $e->getMessage() );
		}
	}

	/**
	 * Show migration progress.
	 *
	 * @param string $type    Migration type.
	 * @param int    $current Current progress.
	 * @return void
	 */
	protected function show_progress( $type, $current ) {
		static $progress;
		static $total;

		if ( ! $progress ) {
			$manifest = get_option( $this->config->get_value( 'slug' ) . '_migration_manifest', [] );
			if ( empty( $manifest ) || ! isset( $manifest[ $type ] ) ) {
				WP_CLI::warning( "No manifest found. Run 'wp etl analyze' first for progress reporting." );
				return;
			}

			$total    = $manifest[ $type ]['count'];
			$progress = \WP_CLI\Utils\make_progress_bar(
				"Processing {$type}",
				$total
			);
		}

		$progress->tick();

		if ( $current >= $total ) {
			$progress->finish();
		}
	}
}

<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('clients', function (Blueprint $table): void {
            $table->id();
            $table->string('phone')->unique();
            $table->timestamps();
        });

        Schema::create('operators', function (Blueprint $table): void {
            $table->id();
            $table->string('name')->nullable();
            $table->boolean('available')->default(true)->index();
            $table->boolean('afk')->default(false)->index();
            $table->unsignedBigInteger('reserved_call_id')->nullable()->index();
            $table->timestamp('reserved_at')->nullable();
            $table->timestamp('last_call_at')->nullable()->index();
            $table->timestamps();
        });

        Schema::create('calls', function (Blueprint $table): void {
            $table->id();
            $table->string('external_call_id')->unique();
            $table->string('phone')->index();
            $table->string('kafka_message_id')->index();
            $table->foreignId('client_id')->nullable()->constrained('clients')->nullOnDelete();
            $table->foreignId('operator_id')->nullable()->constrained('operators')->nullOnDelete();
            $table->string('status', 32)->default('new')->index();
            $table->unsignedInteger('operator_search_attempts')->default(0);
            $table->unsignedInteger('operator_search_max_attempts')->default(1);
            $table->unsignedInteger('operator_search_retry_delay_seconds')->default(0);
            $table->string('operator_search_hangup_policy', 32)->default('missed');
            $table->timestamp('next_operator_search_at')->nullable()->index();
            $table->timestamp('assignment_requested_at')->nullable();
            $table->timestamp('operator_ringing_at')->nullable();
            $table->timestamp('connected_at')->nullable();
            $table->timestamps();
        });

        Schema::create('telephony_outbox', function (Blueprint $table): void {
            $table->id();
            $table->uuid('command_id')->unique();
            $table->string('idempotency_key')->unique();
            $table->string('type', 64)->index();
            $table->string('external_call_id')->index();
            $table->json('payload');
            $table->string('status', 32)->default('pending')->index();
            $table->unsignedInteger('attempts')->default(0);
            $table->timestamp('available_at')->nullable()->index();
            $table->timestamp('published_at')->nullable();
            $table->timestamp('canceled_at')->nullable()->index();
            $table->string('cancel_reason', 128)->nullable();
            $table->text('last_error')->nullable();
            $table->timestamps();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('telephony_outbox');
        Schema::dropIfExists('calls');
        Schema::dropIfExists('operators');
        Schema::dropIfExists('clients');
    }
};
